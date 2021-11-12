"""Module focus on DataStream and its various implementations.

Contains the following classes:
    ``DataStream``
    ``OfflineDataStream``
    ``OnlineDataStream``
"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Iterator

# Third Party Imports
import pandas as pd

# Internal Imports
from .data_sample import DataSample

########################################################################
# Generic Classes
########################################################################

class DataStream():
    """Generic data sample for both offline and online streaming.

    The DataStream class is a generic class that needs to be inherented
    and have its __iter__ and __next__ methods overwritten.

    Raises:
        NotImplementedError: The __iter__ and __next__ functions need to
        be implemented before calling an instance of this class.

    """
    
    def __init__(self, name: str) -> None:
        """Construct the ``DataStream``.

        Args:
            name (str): the name of the data stream.

        """
        self.name = name

    def __repr__(self) -> str:
        """Representation of ``DataStream``.

        Returns:
            str: The representation of ``DataStream``.

        """
        return f"{self.__class__.__name__}: <name={self.name}"

    def __str__(self) -> str:
        """Get String form of ``DataStream``.

        Returns:
            str: The string representation of ``DataStream``.

        """
        return self.__repr__()

    def __iter__(self) -> Iterator[DataSample]:
        """Construct iterator for the ``DataStream``.

        Raises:
            NotImplementedError: __iter__ needs to be overwritten.

        """
        raise NotImplementedError

    def __next__(self) -> DataSample:
        """Get next data sample from ``DataStream``.

        Returns:
            DataSample: The next data sample.

        Raises:
            NotImplementedError: __next__ needs to be overwritten.

        """
        raise NotImplementedError

    def close(self) -> None:
        """Close routine for ``DataStream``."""
        ...

class OfflineDataStream(DataStream):
    """Generic data stream for offline processing. Mostly loading data files.

    OfflineDataStream is intended to be inherented and its __getitem__ 
    method to be overwritten to fit the modality of the actual data.

    Attributes:
        index (int): Keeping track of the current sample to load in __next__.

    Raises:
        NotImplementedError: __getitem__ function needs to be implemented 
        before calling.

    """

    def __init__(self, name: str, timetrack: pd.DataFrame):
        """Construct the OfflineDataStream.

        Args:
            name (str): the name of the data stream.

            timetrack (pd.DataFrame): the time track of the data stream
            where timestamps are provided for each data point.

        """
        super().__init__(name)
        self.timetrack: pd.DataFrame = timetrack
        self.index = 0

    def __iter__(self) -> Iterator[DataSample]:
        """Construct iterator for ``OfflineDataStream``.

        Returns:
            Iterator[DataSample]: The offline data stream's iterator.

        """
        self.index = 0
        return self

    def __next__(self) -> DataSample:
        """Get next data sample from ``OfflineDataStream``.

        Returns:
            DataSample: the next data sample.

        """
        if self.index >= len(self):
            raise StopIteration
        else:
            sample = self.__getitem__(self.index)
            self.index += 1
            return sample
    
    def __getitem__(self, index) -> DataSample:
        """Get indexed data sample from ``OfflineDataStream``.

        Returns:
            DataSample: The indexed data sample.

        Raises:
            NotImplementedError: __getitem__ needs to be overwritten.

        """
        raise NotImplementedError("__getitem__ needs to be implemented.")
    
    def __len__(self) -> int:
        """Get size of ``OfflineDataStream``.

        Returns:
            int: The size of the ``OfflineDataStream``.

        """
        return len(self.timetrack)

    def trim_before(self, trim_time: pd.Timestamp) -> None:
        """Remove data points before the trim_time timestamp.

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

    def trim_after(self, trim_time: pd.Timestamp) -> None:
        """Remove data points after the trim_time timestamp.

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

    def set_index(self, new_index: int) -> None:
        """Set the index used for the __next__ method.

        Args:
            new_index (int): The new index to be set.
        
        """
        self.index = new_index

# class OnlineDataStream(DataStream):
#     """TODO Implementation."""
    
#     def __init__(self, name: str) -> None:
#         self.name = name

#######################################################################

# Constructing useful TypeVar
# DataStreamT: TypeAlias = Union['DataStreamT', DataStream, OfflineDataStream, OnlineDataStream]

#######################################################################
