"""Module focused on Video Data Streams.

Contains the following classes:
    ``VideoDataStream``

"""

# Subpackage Management
__package__ = 'video'

# Built-in Imports
from typing import Union, Tuple, Optional
import pathlib

# Third-party Imports
import cv2
import pandas as pd
import numpy as np

# Internal imports
from pymmdt.data_stream import DataStream

class VideoDataStream(DataStream):
    """Implementation of DataStream focused on Video data.

    Attributes:
        name (str): The name of the data stream.

        video_path (Optional[Union[pathlib.Path, str]]): The path to the video file

        start_time (pd.Timedelta): The timestamp used to dictate the 
        beginning of the video.

    """

    def __init__(self, 
            name:str, 
            start_time:Optional[pd.Timedelta]=None,
            video_path:Optional[Union[pathlib.Path, str]]=None, 
            fps:Optional[int]=None,
            size:Optional[Tuple[int, int]]=None,
            color_mode:Optional[str]="BGR"
        ) -> None:
        """Construct new ``VideoDataStream`` instance.

        Args:
            name (str): The name of the data stream.

            video_path (Optional[Union[pathlib.Path, str]]): The path to the video file

            start_time (pd.Timedelta): The timestamp used to dictate the 
            beginning of the video.

        """
        # Save parameters that don't matter what type of mode ("reading" vs "writing")
        self.color_mode = color_mode

        # First determine if this video is a path or an empty stream
        if video_path:

            # Ensure that the video is a str
            if isinstance(video_path, str):
                video_path = pathlib.Path(video_path)
            
            # Ensure that the file exists
            assert video_path.is_file() and video_path.exists(), "Video file must exists."

            # Even though we use decord for video loading, decord's 
            # method for getting the FPS is faulty so we need to use 
            # OpenCV for this, then we delete this
            self.video = cv2.VideoCapture(str(video_path))
            self.fps = int(self.video.get(cv2.CAP_PROP_FPS))
            self.video.release()

            # Switching to decord because it can read a batch of frames
            import decord # If placed with other imports, it can cause PyQt5 to crash!
            self.video = decord.VideoReader(
                uri=str(video_path), 
                ctx=decord.cpu(0),
                num_threads=1
            )
            self.mode = 'reading'

            # Get the total number of frames
            self.nb_frames = int(len(self.video))
            
            # Constructing timetrack
            # timetrack = pd.date_range(start=start_time, periods=self.nb_frames, freq=f"{int(1e9/self.fps)}N").to_frame()
            self.timeline = pd.TimedeltaIndex(
                pd.timedelta_range(
                    start=start_time, 
                    periods=self.nb_frames, 
                    freq=f"{int(1e9/self.fps)}N"
                )
            )
            
        # Else, this is an empty video data stream
        else:

            # Store the video attributes
            self.fps = fps
            self.size = size
            self.nb_frames = 0
            self.mode = "writing"
            
            # Ensure that the file extension is .mp4
            self.video = cv2.VideoWriter()

            # If path provided, then open the writer fully
            if video_path:
                self.open_writer(video_path)

            # Create an empty timeline
            self.timeline = pd.TimedeltaIndex([])

        # Apply the super constructor
        super().__init__(name, self.timeline)

        # Setting the index is necessary for video, even before __iter__
        self.index = 0
        self.data_index = 0

    @classmethod
    def empty(
            cls, 
            name:str, 
            start_time:Optional[pd.Timedelta]=None, 
            fps:Optional[int]=None, 
            size:Optional[Tuple[int, int]]=None,
            video_path:Optional[Union[pathlib.Path, str]]=None
        ):

        return cls(name, start_time, video_path, fps, size)

    def __len__(self):
        return self.nb_frames

    def open_writer(
        self, 
        filepath:Union[pathlib.Path, str],
        fps:Optional[int]=None,
        size:Optional[int]=None
        ) -> None:
        """Set the video writer by opening with the filepath."""
        assert self.mode == 'writing' and self.nb_frames == 0

        # Check if new fps and size is passed
        if fps:
            self.fps = fps
        if size:
            self.size = size

        # Before opening, check that the neccesary video data is provided
        assert isinstance(self.fps, int) and isinstance(self.size, tuple)

        self.video.open(
            str(filepath),
            cv2.VideoWriter_fourcc(*'DIVX'),
            self.fps,
            (self.size[1], self.size[0])
        )

    def get_frame_size(self) -> Tuple[int, int]:
        """Get the video frame's width and height.

        Returns:
            size (Tuple[int, int]): The frame's width and height.

        """
        if self.mode == "reading": # decord.VideoReader
            size = self.video[0].shape
            w, h = size[1], size[0]
        elif self.mode == "writing": # cv2.VideoCapture
            w = int(self.video.get(3))
            h = int(self.video.get(4))
        else:
            raise RuntimeError("Invalid video mode!")

        return (w, h)

    def get(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> pd.DataFrame:
        assert end_time > start_time, "``end_time`` should be greater than ``start_time``."
        assert self.mode == "reading", "``get`` currently works in ``reading`` mode."
        
        # Generate mask for the window data
        after_start_time = self.timetrack['time'] >= start_time
        before_end_time = self.timetrack['time'] <= end_time
        time_window_mask = after_start_time & before_end_time

        # Obtain the data indicies
        data_idx = self.timetrack[time_window_mask]

        # Check if the data_idx is empty, if so return an empty data frame
        if len(data_idx) == 0:
            return pd.DataFrame()
       
        # Getting the start and end indx to get the frames
        start_data_index = min(data_idx.ds_index)
        end_data_index = max(data_idx.ds_index)
        list_of_frames = list(range(start_data_index, end_data_index+1))

        # Ensure that the video is in the right location
        # self.set_index(start_data_index)

        # Get all the samples
        times = data_idx['time'].tolist()
        stacked_frames = self.video.get_batch(list_of_frames).asnumpy()

        # Convert the data depending on the RGB type
        if self.color_mode == "BGR":
            stacked_frames[...,[0,1,2]] = stacked_frames[...,[2,1,0]]
        elif self.color_mode == "RGB":
            pass

        # Split the stacked frames to a list of frames
        extract_dim_frames = np.split(stacked_frames, len(list_of_frames), axis=0)
        frames = [np.squeeze(x, axis=0) for x in extract_dim_frames]

        # Update the data index record
        # very important to update - as this keeps track of the video's 
        # location
        self.data_index = end_data_index + 1

        # Construct data frame
        df = pd.DataFrame({'_time_': times, 'frames': frames})

        return df

    def set_index(self, new_data_index):
        """Set the video's index by updating the pointer in OpenCV."""
        # If the data index does not match the requested index,
        # it means that some jump or cut has happend.
        # We need to clear our the reading queue and set the video.
        if self.data_index != new_data_index:
            print(f"Video miss - reassigning index: {self.data_index}-{new_data_index}")
            if self.mode == "reading":

                # Set the new location for the video
                self.video.set(cv2.CAP_PROP_POS_FRAMES, new_data_index-1)
                self.data_index = new_data_index

    def append(
            self, 
            append_data:Union[pd.DataFrame, pd.Series], 
            time_column:str='_time_',
            data_column:str='frames'
        ):
        # The append_data needs to have timedelta_index column
        assert time_column in append_data.columns
        time_column_data = append_data[time_column]
        assert isinstance(time_column_data.iloc[0], pd.Timedelta), "time column should be ``pd.Timedelta`` objects."

        # Check that the data in the frames is indeed np.arrays
        assert isinstance(append_data[data_column].iloc[0], np.ndarray), "appending data needs to be store in column ``frames`` as a np.array."

        # This operation can only be done in the writing mode
        assert self.mode == "writing"

        # Create data frame for the timeline
        append_timetrack = pd.DataFrame(
            {
                'time': time_column_data, 
                'ds_index': 
                    [x for x in range(len(time_column_data))]
            }
        )

        # Add to the timetrack (cannot be inplace)
        self.timetrack = self.timetrack.append(append_timetrack)
        self.timetrack['ds_index'] = self.timetrack['ds_index'].astype(int)

        # Appending the file to the video writer
        for index, row in append_data.iterrows():
            frame = getattr(row, data_column)
            self.video.write(frame.copy())
            self.nb_frames += 1

    def close(self):
        """Close the ``VideoDataStream`` instance."""
        # Closing the video capture device
        if self.mode == "reading": # Decord
            pass
        elif self.mode == "writing": # OpenCV
            self.video.release()
        else:
            raise RuntimeError("Invalid mode for video data stream.")
