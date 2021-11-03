# Package Management
__package__ = 'mm'

# Built-in Imports
import pandas as pd

class DataSample:
    """Standard output data type for Data Streams.

    Args:
        dtype (str): Typically the name of the parent DataStream
        data (Any): The data content of the sample
        time (pd.Timestamp): the timestamp associated with the sample
    
    Attributes:
        dtype (str): Typically the name of the parent DataStream
        data (Any): The data content of the sample
        time (pd.Timestamp): the timestamp associated with the sample

    """
 
    def __init__(self, dtype:str, time:pd.Timestamp, data):
       self.dtype = dtype
        self.data = data
        self.time = time

    def __repr__(self):
        return f"DataSample: <dtype={self.dtype}>, <time={type(self.time)}>"

    def __str__(self):
        return self.__repr__()
