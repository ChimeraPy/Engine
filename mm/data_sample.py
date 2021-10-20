# Package Management
__package__ = 'mm'

# Built-in Imports
import pandas as pd

# Internal Imports
from .data_stream import DataStream

class DataSample:

    def __init__(self, dtype:str, time:pd.Timestamp, pointer:int, data: DataStream):
        self.dtype = dtype
        self.data_stream = data
        self.data_pointer = pointer
        self.time = time

    def load_data(self):
        self.data = self.data_stream[self.data_pointer]

    def __repr__(self):
        return f"DataSample: <dtype={self.dtype}>, <time={type(self.time)}>"

    def __str__(self):
        return self.__repr__()
