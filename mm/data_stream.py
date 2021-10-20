# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import Union
import pathlib
import datetime

# Third Party Imports
import pandas as pd
import cv2

########################################################################
# Generic Classes
########################################################################

class DataStream():
    
    def __init__(self, name):
        self.name = name

class OfflineDataStream(DataStream):
    
    def __getitem__(self, index):
        raise NotImplementedError

########################################################################
# Implementation Classes
########################################################################

class OfflineCSVDataStream(OfflineDataStream):

    def __init__(
            self, name: str, 
            data: pd.DataFrame, 
            time_column: str, 
            data_columns: list
        ):
        super().__init__(name)
        self.data = data
        self.time_column = time_column
        self.data_columns = data_columns

    def get_timestamps(self) -> pd.DatetimeIndex:
        return pd.DatetimeIndex(self.data[self.time_column])

    def __getitem__(self, index):
        return self.data.iloc[index]
    
    def __len__(self):
        return len(self.data)

class OfflineVideoDataStream(OfflineDataStream):
    
    def __init__(self, name: 
            str, video_path: Union[pathlib.Path, str], 
            start_time: datetime.datetime
        ):
        super().__init__(name)

        # Ensure that the video is a str
        if isinstance(video_path, pathlib.Path):
            video_path = str(video_path)

        # constructing video capture object
        self.video_cap = cv2.VideoCapture(video_path)

        # Obtaining FPS and total number of frames
        self.fps = int(self.video_cap.get(cv2.CAP_PROP_FPS))
        self.nb_frames = int(self.video_cap.get(cv2.CAP_PROP_FRAME_COUNT))

        # Constructing timestamps for each frame
        self.timestamps = pd.date_range(start=start_time, periods=self.nb_frames, freq=f"{int(1e9/self.fps)}N")

        # Index for tracking the current location of the frame reader
        self.index = 0

    def get_timestamps(self) -> pd.DatetimeIndex:
        return self.timestamps

    def __getitem__(self, index):
        # Only if the index does not match request index should we 
        # change the location of the buffer reader
        if self.index != index:
            self.video_cap.set(cv2.CAP_PROP_POS_FRAMES, index-1)

        # Load data
        res, frame = self.video_cap.read()

        # Mark the change in the index
        self.index += 1

        # Return data
        return frame

    def __len__(self):
        return self.nb_frames

