"""Module focused on Video Data Streams.

Contains the following classes:
    ``VideoDataStream``

"""

# Subpackage Management
__package__ = 'video'

from typing import Union, Tuple, Optional
import pathlib

import cv2
import pandas as pd
import numpy as np

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
            size: Optional[Tuple[int, int]]=None
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
            timeline = pd.TimedeltaIndex(
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
            timeline = pd.TimedeltaIndex([])

        # Apply the super constructor
        super().__init__(name, timeline)

        # Setting the index is necessary for video, even before __iter__
        self.index = 0
        self.data_index = 0

    @classmethod
    def empty(
            cls, 
            name:str, 
            start_time:pd.Timedelta, 
            fps:int, 
            size:Tuple[int, int]
        ):

        return cls(name, start_time, None, fps, size)

    def open_writer(self, filepath: pathlib.Path) -> None:
        """Set the video writer by opening with the filepath."""
        assert self.mode == 'writing' and self.nb_frames == 0
        self.video.open(
            str(filepath),
            cv2.VideoWriter_fourcc(*'DIVX'),
            self.fps,
            self.size
        )

    def get_frame_size(self) -> Tuple[int, int]:
        """Get the video frame's width and height.

        Returns:
            size (Tuple[int, int]): The frame's width and height.

        """
        frame_width = int(self.video.get(3))
        frame_height = int(self.video.get(4))
        return (frame_width, frame_height)

    def __getitem__(self, index):
        """Get the indexed data sample from the ``VideoDataStream``.

        Args:
            index (int): The requested index.

        Returns:
            DataSample: The indexed data sample.

        """
        # Only reading is allowed to access frames
        assert self.mode == "reading"

        # Only if the index does not match request index should we 
        # change the location of the buffer reader
        # Convert table index to data index
        data_index = self.timetrack.iloc[index]['ds_index']

        # Ensure that the video index is correct
        self.set_index(data_index)

        # Load data
        res, frame = self.video.read()
        self.data_index += 1

        # Return frame
        return frame

    def set_index(self, new_index):
        """Set the video's index by updating the pointer in OpenCV."""
        if self.data_index != new_index:
            self.video.set(cv2.CAP_PROP_POS_FRAMES, new_index-1)
            self.data_index = new_index

    def append(self, timestamp: pd.Timedelta, frame: np.ndarray):
        assert self.mode == "writing"

        # Add to the timetrack (cannot be inplace)
        self.timetrack = self.timetrack.append({
            'time': timestamp, 
            'ds_index': int(len(self.timetrack))
        }, ignore_index=True)

        # Appending the file to the video writer
        self.video.write(frame)
        self.nb_frames += 1

    def close(self):
        """Close the ``VideoDataStream`` instance."""
        # Closing the video capture device
        self.video.release()
