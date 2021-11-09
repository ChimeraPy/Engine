"""Module focused on ``DataSample`` implementation.

Contains the following classes:
    ``DataSample``

"""

# Package Management
__package__ = 'mm'

# Built-in Imports
import pandas as pd

class DataSample:
    """Standard output data type for Data Streams.
 
    Attributes:
        dtype (str): Typically the name of the parent DataStream.

        data (Any): The data content of the sample.

        time (pd.Timestamp): the timestamp associated with the sample.

    """
 
    def __init__(self, dtype:str, time:pd.Timestamp, data) -> None:
        """Construct the ``DataSample``.

        Args:
            dtype (str): Typically the name of the parent DataStream.

            data (Any): The data content of the sample.

            time (pd.Timestamp): the timestamp associated with the sample.

        """
        self.dtype = dtype
        self.data = data
        self.time = time

    def __repr__(self) -> str:
        """Get representation of ``DataSample``.

        Returns:
            self_print (str): The string representation of ``DataSample``.

        """
        return f"DataSample: <dtype={self.dtype}>, <time={type(self.time)}>"

    def __str__(self) -> str:
        """Get representation of ``DataSample``.

        Returns:
            self_print (str): The string representation of ``DataSample``.

        """
        return self.__repr__()
