# Subpackage Management
__package__ = 'video'

# Built-in Imports
from typing import Union, Tuple, Optional
import multiprocessing as mp
import pathlib
import gc

# Third-party Imports
# import vidgear.gears
import cv2
import pandas as pd
import numpy as np

# Internal imports
from chimerapy.core.data_stream import DataStream

class VideoDataStream(DataStream):
    """Implementation of DataStream focused on Video data.

    Attributes:
        name (str): The name of the data stream.
        video_path (Optional[Union[pathlib.Path, str]]): The path to \
            the video file
        start_time (pd.Timedelta): The timestamp used to dictate the \
            beginning of the video.

    """

    def __init__(self, 
            name:str, 
            start_time:pd.Timedelta=pd.Timedelta(seconds=0),
            video_path:Optional[Union[pathlib.Path, str]]=None, 
            fps:Optional[Union[float,int]]=None,
            size:Optional[Tuple[int, int]]=None,
            color_mode:str="BGR",
            startup_now:bool=False
        ) -> None:
        """Construct new ``VideoDataStream`` instance.

        Args:
            name (str): The name of the data stream.
            video_path (Optional[Union[pathlib.Path, str]]): The path to \
                the video file.
            start_time (pd.Timedelta): The timestamp used to dictate the \
                beginning of the video.

        """
        # Save parameters that don't matter what type of mode ("reading" vs "writing")
        self.name = name
        self.start_time = start_time
        self.video_path = video_path
        self.fps = fps
        self.size = size
        self.color_mode = color_mode
        self.has_startup = False
        self.compression_queue = mp.Queue(maxsize=5)
        
        # Setting the index is necessary for video, even before __iter__
        self.index = 0
        self.data_index = 0

        # First determine if this video is a path or an empty stream
        if self.video_path:

            # This is the "reading" mode
            self.mode = "reading"

            # Ensure that the video is a str
            if isinstance(self.video_path, str):
                self.video_path = pathlib.Path(self.video_path)
            
            # Ensure that the file exists
            assert self.video_path.is_file() and self.video_path.exists(), "Video file must exists."

        else:

            # This is the "writing" mode
            self.mode = "writing"

        # Create an empty timeline
        self.timeline = pd.TimedeltaIndex([])

        # Apply the super constructor
        super().__init__(name, self.timeline)

        # If starting now, then load the video
        if startup_now:
            self.startup()

    @classmethod
    def empty(
            cls, 
            name:str, 
            start_time:Optional[pd.Timedelta]=None, 
            fps:Optional[Union[float,int]]=None, 
            size:Optional[Tuple[int, int]]=None,
            video_path:Optional[Union[pathlib.Path, str]]=None,
            startup_now:bool=False
        ):
        """Create an empty video data stream.

        Args:
            name (str): Name to give to the video data stream.
            start_time (Optional[pd.Timedelta]): Start time of the video \
                data stream.
            fps (Optional[Union[float,int]]): The fps to save video.
            size (Optional[Tuple[int, int]]): The height and width of \
                the video.
            video_path (Optional[Union[pathlib.Path, str]]): Path to \
                store the video.
            startup_now (bool): Should we start up the video data stream?
        """

        return cls(
            name=name, 
            start_time=start_time, 
            video_path=video_path, 
            fps=fps, 
            size=size, 
            startup_now=startup_now
        )

    def startup(self):

        # This is for the reading mode only!
        if self.mode == "reading":

            # Create the cv2 object now since it cannot be seralized and 
            # therefore be passed to a process. We have to create it inside
            # the ``run`` method of the process.
            self.video = cv2.VideoCapture(str(self.video_path))
            self.nb_frames = int(self.video.get(cv2.CAP_PROP_FRAME_COUNT))

            # Only update fps if there was no previous fps set
            if type(self.fps) == type(None):
                self.fps = self.video.get(cv2.CAP_PROP_FPS)


            # Now that we have video len size, we can update the video
            # data stream's timetrack
            self.update_timetrack()

        # Else, its for the writing mode
        else:

            # Then open the writer
            self.video = cv2.VideoWriter()
            self.nb_frames = 0

        # After creating the timetrack, we need to apply the trim
        super().startup()

        # Update the flag variable
        self.has_startup = True

    def __len__(self):
        return self.nb_frames

    def set_start_time(self, start_time:pd.Timedelta):
        self.start_time = start_time

    def shift_start_time(self, diff_time:pd.Timedelta):
        self.start_time += diff_time

    def set_fps(self, fps:int):
        self.fps = fps

    def update_timetrack(self):
            
        # Creating new timeline
        self.timeline = pd.TimedeltaIndex(
            pd.timedelta_range(
                start=self.start_time, 
                periods=self.nb_frames, 
                freq=f"{int(1e9/self.fps)}N"
            )
        )

        # Converting timeline to timetrack
        super().make_timetrack(self.timeline)

    def open_writer(
        self, 
        video_path:Union[pathlib.Path, str],
        fps:Optional[Union[float,int]]=None,
        size:Optional[Tuple[int, int]]=None,
        grey=False
        ) -> None:
        """Set the video writer by opening with the filepath."""
        assert self.mode == 'writing'
        assert self.has_startup == True, f"{self.__class__.__name__} cannot execute ``open_writer`` before calling ``startup``."

        # Storing information
        self.video_path = video_path

        # Check if new fps and size is passed
        if fps:
            self.fps = fps
        if size:
            self.size = size

        # Before opening, check that the neccesary video data is provided
        assert isinstance(self.fps, (float, int)) and isinstance(self.size, tuple)

        # Opening the video writer given the desired parameters
        if not grey:
            self.video.open(
                str(self.video_path),
                cv2.VideoWriter_fourcc(*'DIVX'),
                self.fps,
                (self.size[1], self.size[0])
            )
        else:
            self.video.open(
                str(self.video_path),
                cv2.VideoWriter_fourcc(*'DIVX'),
                self.fps,
                (self.size[1], self.size[0]),
                0 # This is to save grey video
            )

    def get_frame_size(self) -> Tuple[int, int]:
        """Get the video frame's width and height.

        Returns:
            size (Tuple[int, int]): The frame's width and height.

        """
        assert self.has_startup == True, f"{self.__class__.__name__} cannot execute ``get_frame_size`` before calling ``startup``."

        if isinstance(self.video, (cv2.VideoCapture, cv2.VideoWriter)):
            w = int(self.video.get(3))
            h = int(self.video.get(4))
        else:
            raise RuntimeError("Invalid self.video object")

        return (w, h)

    def get(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> pd.DataFrame:
        """Get video data from ``start_time`` to ``end_time``.

        Args:
            start_time (pd.Timedelta): Start of the time window.
            end_time (pd.Timedelta): End of the time window.

        Returns:
            pd.DataFrame: video data from time window.

        """
        assert end_time > start_time, "``end_time`` should be greater than ``start_time``."
        assert self.mode == "reading", "``get`` currently works in ``reading`` mode."
        assert self.has_startup == True, f"{self.__class__.__name__} cannot execute ``get`` before calling ``startup``."
        
        # Generate mask for the window data
        after_start_time = self.timetrack['time'] >= start_time
        before_end_time = self.timetrack['time'] < end_time
        time_window_mask = after_start_time & before_end_time

        # Obtain the data indicies
        data_idx = self.timetrack[time_window_mask]

        # Check if the data_idx is empty, if so return an empty data frame
        if len(data_idx) == 0:
            return pd.DataFrame({
                '_time_':pd.TimedeltaIndex([]), 
                'frames':[]}
            )
       
        # Getting the start and end indx to get the frames
        start_data_index = min(data_idx.ds_index)
        end_data_index = max(data_idx.ds_index)
        list_of_frames = list(range(start_data_index, end_data_index+1))

        # Ensure that the video is in the right location
        self.set_index(start_data_index)

        # Get all the samples
        times = data_idx['time'].tolist()
        # stacked_frames = self.video.get_batch(list_of_frames).asnumpy() # decord does not play nice with PyQt5
        frames = []
        for i in range(start_data_index, end_data_index+1):
            res, frame = self.video.read()
            frames.append(frame)

        # Convert the data depending on the RGB type
        if self.color_mode == "RGB":

            # Stack to easily change color channels
            stacked_frames = np.stack(frames)
            stacked_frames[...,[0,1,2]] = stacked_frames[...,[2,1,0]]

            # Split the stacked frames to a list of frames
            extract_dim_frames = np.split(stacked_frames, len(list_of_frames), axis=0)
            frames = [np.squeeze(x, axis=0) for x in extract_dim_frames]

        elif self.color_mode == "BGR":
            pass

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
            # print(f"Video miss - reassigning index: {self.data_index}-{new_data_index}")
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
            self.video.write(np.uint8(frame).copy())
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
