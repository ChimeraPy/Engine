# Package Management
__package__ = 'chimerapy'

# Built-in Imports

# Third Party Imports
import pandas as pd

# Internal Imports

class DataStream:
    """Generic data stream for processing. 

    ``DataStream`` is intended to be inherented and its __getitem__ \
        method to be overwritten to fit the modality of the actual data.

    Attributes:
        index (int): Keeping track of the current sample to load in __next__.

    Raises:
        NotImplementedError: __getitem__ function needs to be implemented \
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
        self.before_trim_time = None
        self.after_trim_time = None
        
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

    def __len__(self) -> int:
        """Get size of ``DataStream``.

        Returns:
            int: The size of the ``DataStream``.

        """
        return len(self.timetrack)
    
    def set_before_trim_time(self, trim_time:pd.Timedelta):
        """Setting the time where anything before is removed.

        Args:
            trim_time (pd.Timedelta): Cutoff time.

        """
        self.before_trim_time = trim_time

    def set_after_trim_time(self, trim_time:pd.Timedelta):
        """Setting the time where anything after is removed.

        Args:
            trim_time (pd.Timedelta): Cutoff time.
        """
        self.after_trim_time = trim_time

    def startup(self):
        """Start the data stream by applying the before and after trims."""

        # Assuming that the timetrack has been already created
        if isinstance(self.before_trim_time, pd.Timedelta):
            self.trim_before(self.before_trim_time)

        if isinstance(self.after_trim_time, pd.Timedelta):
            self.trim_after(self.after_trim_time)

    def make_timetrack(self, timeline: pd.TimedeltaIndex):
        """Convert the data stream's timeline to a timetrack.

        Args:
            timeline (pd.TimedeltaIndex): The simple timeline of the 
            data stream.
        
        """
        # Constructing the timetrack (including time and data pointer)
        self.timetrack = pd.DataFrame({
            'time': timeline,
            'ds_index': [x for x in range(len(timeline))]
        })

    def get(
        self, 
        start_time: pd.Timedelta, 
        end_time: pd.Timedelta
        ) -> pd.DataFrame:
        """Get the data samples from the time window.

        Args:
            start_time (pd.Timedelta): The start time where the time \
                window is loaded from.
            end_time (pd.Timedelta): The end time where the time window \
                is loaded from.

        Raises: 
            NotImplementedError: ``get`` needs to be implemented in a \
                concrete child of the ``DataStream``.

        Returns:
            pd.DataFrame: The data frame containing all data samples \
                found within the time window.
        """
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

    def append(self, timestamp: pd.Timedelta, sample: pd.DataFrame) -> None:
        """Add a data sample to the data stream

        Raises:
            NotImplementedError: ``append`` needs to be overwritten.

        """
        raise NotImplementedError("``append`` needs to be implemented.")

    def close(self) -> None:
        """Close routine for ``DataStream``."""
        ...
