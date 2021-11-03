# Package Management
__package__ = 'mm'

# Built-in Imports
from typing import Union
import pathlib
import datetime
import collections
import types
import functools

# Third Party Imports
import tqdm
import pandas as pd
import cv2

# Internal Imports
from .data_sample import DataSample
from .process import Process

########################################################################
# Generic Classes
########################################################################

class DataStream():
    """Generic data sample for both offline and online streaming.

    The DataStream class is a generic class that needs to be inherented
    and have its __iter__ and __next__ methods overwritten.

    Args:
        name (str): the name of the data stream

    Raises:
        NotImplementedError: The __iter__ and __next__ functions need to
        be implemented before calling an instance of this class.

    """
    
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"{self.__class__.__name__}: <name={self.name}"

    def __str__(self):
        return self.__repr__()

    def __iter__(self):
        raise NotImplementedError

    def __next__(self):
        raise NotImplementedError

    def close(self):
        ...

class OfflineDataStream(DataStream):
    """Generic data stream for offline processing. Mostly loading data files.

    OfflineDataStream is intended to be inherented and its __getitem__ 
    method to be overwritten to fit the modality of the actual data.

    Args:
        name (str): the name of the data stream.
        timetrack (pd.DataFrame): the time track of the data stream
        where timestamps are provided for each data point.

    Attributes:
        index (int):

    Raises:
        NotImplementedError: __getitem__ function needs to be implemented 
        before calling.

    """

    def __init__(self, name: str, timetrack: pd.DataFrame):
        super().__init__(name)
        self.timetrack = timetrack
        self.index = 0

    def __iter__(self):
        self.index = 0
        return self

    def __next__(self) -> DataSample:
        if self.index >= len(self):
            raise StopIteration
        else:
            sample = self.__getitem__(self.index)
            self.index += 1
            return sample
    
    def __getitem__(self, index):
        raise NotImplementedError("__getitem__ needs to be implemented.")
    
    def __len__(self):
        return len(self.timetrack)

    def trim_before(self, trim_time: pd.Timestamp):
        """Removes data points before the trim_time timestamp.

        Args:
            trim_time (pd.Timestamp): The cut-off time. 

        """

        # Obtain the mask 
        mask = self.timetrack['time'] > trim_time
        new_timetrack = self.timetrack[mask]
        new_timetrack.reset_index(inplace=True)
        new_timetrack = new_timetrack.drop(columns=['index'])

        # Store the new timetrack
        self.timetrack = new_timetrack

    def trim_after(self, trim_time: pd.Timestamp):
        """Removes data points after the trim_time timestamp.

        Args:
            trim_time (pd.Timestamp): The cut-off time.

        """

        # Obtain the mask 
        mask = self.timetrack['time'] < trim_time
        new_timetrack = self.timetrack[mask]
        new_timetrack.reset_index(inplace=True)
        new_timetrack = new_timetrack.drop(columns=['index'])
        
        # Store the new timetrack
        self.timetrack = new_timetrack

    def set_index(self, new_index: int):
        """Sets the index used for the __next__ method

        Args:
            new_index (int): The new index to be set.
        
        """
        self.index = new_index
