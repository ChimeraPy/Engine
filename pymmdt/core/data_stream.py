"""Module focus on DataStream and its various implementations.

Contains the following classes:
    ``DataStream``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Iterator, Any

# Third Party Imports
import pandas as pd

# Internal Imports

########################################################################
# Generic Classes
########################################################################

class DataStream:
    """Generic data stream for processing. 

    ``DataStream`` is intended to be inherented and its __getitem__ 
    method to be overwritten to fit the modality of the actual data.

    Attributes:
        index (int): Keeping track of the current sample to load in __next__.

    Raises:
        NotImplementedError: __getitem__ function needs to be implemented 
        before calling.

    """

    def __init__(
            self, 
            name: str, 
            timeline: pd.TimedeltaIndex
        ):
        """Construct the DataStream.

        Args:
            name (str): the name of the data stream.

            timetrack (pd.DataFrame): the time track of the data stream
            where timestamps are provided for each data point.

        """
        self.name = name
        self.index = 0
        
        # Creating timetrack from timeline
        self.make_timetrack(timeline)
    
    def __repr__(self) -> str:
        """Representation of ``DataStream``.

        Returns:
            str: The representation of ``DataStream``.

        """
        return f"{self.__class__.__name__}: <name={self.name}>"

    def __str__(self) -> str:
        """Get String form of ``DataStream``.

        Returns:
            str: The string representation of ``DataStream``.

        """
        return self.__repr__()

    def __iter__(self) -> Iterator[Any]:
        """Construct iterator for ``DataStream``.

        Returns:
            Iterator[DataSample]: The offline data stream's iterator.

        """
        self.index = 0
        return self

    def __next__(self) -> Any:
        """Get next data sample from ``DataStream``.

        Returns:
            DataSample: the next data sample.

        """
        if self.index >= len(self):
            raise StopIteration
        else:
            sample = self.__getitem__(self.index)
            self.index += 1
            return sample
    
    def __getitem__(self, index) -> Any:
        """Get indexed data sample from ``Any``.

        Returns:
            DataSample: The indexed data sample.

        Raises:
            NotImplementedError: __getitem__ needs to be overwritten.

        """
        raise NotImplementedError("__getitem__ needs to be implemented.")
    
    def __len__(self) -> int:
        """Get size of ``DataStream``.

        Returns:
            int: The size of the ``DataStream``.

        """
        return len(self.timetrack)

    def startup(self):
        pass

    def make_timetrack(self, timeline: pd.TimedeltaIndex):

        # Constructing the timetrack (including time and data pointer)
        self.timetrack = pd.DataFrame({
            'time': timeline,
            'ds_index': [x for x in range(len(timeline))]
        })

    def get(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> pd.DataFrame:
        raise NotImplementedError

    def trim_before(self, trim_time: pd.Timedelta) -> None:
        """Remove data points before the trim_time timestamp.

        Args:
            trim_time (pd.Timedelta): The cut-off time. 

        """
        # Obtain the mask 
        mask = self.timetrack['time'] > trim_time
        new_timetrack = self.timetrack[mask]
        new_timetrack.reset_index(inplace=True)
        new_timetrack = new_timetrack.drop(columns=['index'])

        # Store the new timetrack
        self.timetrack = new_timetrack

    def trim_after(self, trim_time: pd.Timedelta) -> None:
        """Remove data points after the trim_time timestamp.

        Args:
            trim_time (pd.Timedelta): The cut-off time.

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
    
    @classmethod
    def empty(cls):
        """Create an empty data stream.

        Raises:
            NotImplementedError: empty needs to be overwritten.

        """
        raise NotImplementedError("``empty`` needs to be implemented.")

    def append(self, timestamp: pd.Timedelta, sample: Any) -> None:
        """Add a data sample to the data stream

        Raises:
            NotImplementedError: ``append`` needs to be overwritten.

        """
        raise NotImplementedError("``append`` needs to be implemented.")

    def close(self) -> None:
        """Close routine for ``DataStream``."""
        ...
