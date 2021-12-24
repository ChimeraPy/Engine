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
            name: str, 
            start_time: pd.Timedelta,
            video_path: Optional[Union[pathlib.Path, str]]=None, 
            fps: Optional[int]=None,
            size: Optional[Tuple[int, int]]=None,
            max_queue_size: int=1000,
        ) -> None:
        """Construct new ``VideoDataStream`` instance.

        Args:
            name (str): The name of the data stream.

            video_path (Optional[Union[pathlib.Path, str]]): The path to the video file

            start_time (pd.Timedelta): The timestamp used to dictate the 
            beginning of the video.

        """
        # First determine if this video is a path or an empty stream
        if video_path:

            # Ensure that the video is a str
            if isinstance(video_path, str):
                video_path = pathlib.Path(video_path)
            
            # Ensure that the file exists
            assert video_path.is_file() and video_path.exists(), "Video file must exists."

            # constructing video capture object
            self.video = cv2.VideoCapture(str(video_path))
            self.mode = 'reading'

            # Obtaining FPS and total number of frames
            self.fps = int(self.video.get(cv2.CAP_PROP_FPS))
            self.nb_frames = int(self.video.get(cv2.CAP_PROP_FRAME_COUNT))
            
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
            start_time:pd.Timedelta, 
            fps:int, 
            size:Tuple[int, int],
            max_queue_size:int=1000
        ):

        return cls(name, start_time, None, fps, size, max_queue_size)

    def open_writer(self, filepath: pathlib.Path) -> None:
        """Set the video writer by opening with the filepath."""
        assert self.mode == 'writing' and self.nb_frames == 0
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
        frame_width = int(self.video.get(3))
        frame_height = int(self.video.get(4))
        return (frame_width, frame_height)

    def get(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> pd.DataFrame:
        
        # Generate mask for the window data
        after_start_time = self.timetrack['time'] >= start_time
        before_end_time = self.timetrack['time'] < end_time
        time_window_mask = after_start_time & before_end_time

        # Obtain the data indicies
        data_idx = self.timetrack[time_window_mask]
        start_data_index = min(data_idx.ds_index)
        end_data_index = max(data_idx.ds_index)

        # Ensure that the video is in the right location
        self.set_index(start_data_index)

        # Get all the samples
        times = []
        frames = []
        for idx, data in data_idx.iterrows():
            timestamp = data['time']
            frame = self.video.read()

            times.append(timestamp)
            frames.append(frame)

        # Update the data index record
        # very important to update - as this keeps track of the video's 
        # location
        self.data_index = end_data_index + 1

        # Construct data frame
        df = pd.DataFrame({'time': times, 'frames': frames})

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

    def append(self, timestamp: pd.Timedelta, frame: np.ndarray):
        assert self.mode == "writing"

        # Add to the timetrack (cannot be inplace)
        self.timetrack = self.timetrack.append({
            'time': timestamp, 
            'ds_index': int(len(self.timetrack))
        }, ignore_index=True)

        # Appending the file to the video writer
        self.video.write(frame.copy())
        self.nb_frames += 1

    def close(self):
        """Close the ``VideoDataStream`` instance."""
        # Closing the video capture device
        self.video.release()
