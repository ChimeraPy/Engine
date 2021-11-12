"""Module focused on Video Data Streams.

Contains the following classes:
    ``OfflineVideoDataStream``

"""

# Subpackage Management
__package__ = 'video'

from typing import Union, Tuple
import pathlib

import cv2
import pandas as pd

from pymmdt.data_stream import OfflineDataStream
from pymmdt.data_sample import DataSample

class OfflineVideoDataStream(OfflineDataStream):
    """Implementation of Offline DataStream focused on Video data.

    Attributes:
        name (str): The name of the data stream.

        video_path (Union[pathlib.Path, str]): The path to the video file.

        start_time (pd.Timestamp): The timestamp used to dictate the 
        beginning of the video.

    """

    def __init__(self, 
            name: str, 
            video_path: Union[pathlib.Path, str], 
            start_time: pd.Timestamp
        ) -> None:
        """Construct new ``OfflineVideoDataStream`` instance.

        Args:
            name (str): The name of the data stream.

            video_path (Union[pathlib.Path, str]): The path to the video file

            start_time (pd.Timestamp): The timestamp used to dictate the 
            beginning of the video.

        """
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
        timetrack.drop(columns=['index'], inplace=True)

        # Apply the super constructor
        super().__init__(name, timetrack)

        # Setting the index is necessary for video, even before __iter__
        self.index = 0
        self.data_index = 0

    def get_size(self) -> Tuple[int, int]:
        """Get the video frame's width and height.

        Returns:
            size (Tuple[int, int]): The frame's width and height.

        """
        frame_width = int(self.video_cap.get(3))
        frame_height = int(self.video_cap.get(4))
        return (frame_width, frame_height)

    def set_index(self, new_index):
        """Set the video's index by updating the pointer in OpenCV."""
        if self.data_index != new_index:
            self.video_cap.set(cv2.CAP_PROP_POS_FRAMES, new_index-1)
            self.data_index = new_index

    def __getitem__(self, index) -> DataSample:
        """Get the indexed data sample from the ``OfflineVideoDataStream``.

        Args:
            index (int): The requested index.

        Returns:
            DataSample: The indexed data sample.

        """
        # Only if the index does not match request index should we 
        # change the location of the buffer reader
        # Convert table index to data index
        data_index = self.timetrack.iloc[index]['ds_index']

        # Ensure that the video index is correct
        self.set_index(data_index)

        # Load data
        res, frame = self.video_cap.read()
        self.data_index += 1

        # Creating a DataSample
        data_sample = DataSample(
            dtype=self.name,
            time=self.timetrack.iloc[index]['time'],
            data=frame
        )

        # Return frame
        return data_sample

    def close(self):
        """Close the ``OfflineVideoDataStream`` instance."""
        # Closing the video capture device
        self.video_cap.release()
