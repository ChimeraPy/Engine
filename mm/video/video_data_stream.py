# Subpackage Management
__package__ = 'video'

from typing import Union, Tuple
import pathlib

import cv2
import pandas as pd
import datetime

from mm.data_stream import OfflineDataStream
from mm.data_sample import DataSample

class OfflineVideoDataStream(OfflineDataStream):
    """Implementation of Offline DataStream focused on Video data.

    Args:
        name (str): The name of the data stream.
        video_path (Union[pathlib.Path, str]): The path to the video file
        start_time (pd.Timestamp): The timestamp used to dictate the 
        beginning of the video.

    Attributes:
        name (str): The name of the data stream.
        video_path (Union[pathlib.Path, str]): The path to the video file
        start_time (pd.Timestamp): The timestamp used to dictate the 
        beginning of the video.
    """
    def __init__(self, 
            name: str, 
            video_path: Union[pathlib.Path, str], 
            start_time: pd.Timestamp
        ):

        # Ensure that the video is a str
        if isinstance(video_path, pathlib.Path):
            video_path = str(video_path)

        # constructing video capture object
        self.video_cap = cv2.VideoCapture(video_path)

        # Obtaining FPS and total number of frames
        self.fps = int(self.video_cap.get(cv2.CAP_PROP_FPS))
        self.nb_frames = int(self.video_cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Constructing timetrack
        timetrack = pd.date_range(start=start_time, periods=self.nb_frames, freq=f"{int(1e9/self.fps)}N").to_frame()
        timetrack.columns = ['time']
        timetrack['ds_index'] = [x for x in range(self.nb_frames)]
        timetrack.reset_index(inplace=True)
        timetrack = timetrack.drop(columns=['index'])

        # Apply the super constructor
        super().__init__(name, timetrack)

        # Setting the index is necessary for video, even before __iter__
        self.index = 0

    def get_size(self) -> Tuple[int, int]:
        """Get the video frame's width and height.

        Returns:
            size (Tuple[int, int]): The frame's width and height.

        """
        frame_width = int(self.video_cap.get(3))
        frame_height = int(self.video_cap.get(4))
        return (frame_width, frame_height)

    def set_index(self, new_index):
        """Set's the video's index by updating the pointer in OpenCV."""
        if self.index != new_index:
            self.video_cap.set(cv2.CAP_PROP_POS_FRAMES, new_index-1)
            self.index = new_index

    def __getitem__(self, index) -> DataSample:
        # Only if the index does not match request index should we 
        # change the location of the buffer reader
        if self.index != index:
            self.video_cap.set(cv2.CAP_PROP_POS_FRAMES, index-1)
            self.index = index

        # Load data
        res, frame = self.video_cap.read()

        # Creating a DataSample
        data_sample = DataSample(
            dtype=self.name,
            time=self.timetrack.iloc[index]['time'],
            data=frame
        )

        # Return frame
        return data_sample

    def close(self):
        # Closing the video capture device
        self.video_cap.release()
