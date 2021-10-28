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

class OfflineDataStream(DataStream):

    def __init__(self, name: str, timetrack: pd.DataFrame):
        super().__init__(name)
        self.timetrack = timetrack

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

        # Obtain the mask 
        mask = self.timetrack['time'] > trim_time
        new_timetrack = self.timetrack[mask]
        new_timetrack.reset_index()

        # Store the new timetrack
        self.timetrack = new_timetrack

    def trim_after(self, trim_time: pd.Timestamp):

        # Obtain the mask 
        mask = self.timetrack['time'] < trim_time
        new_timetrack = self.timetrack[mask]
        new_timetrack.reset_index()

        # Store the new timetrack
        self.timetrack = new_timetrack
