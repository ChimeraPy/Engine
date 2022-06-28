# Package Management
__package__ = 'chimerapy'

# Built-in Imports

# Third Party Imports
import multiprocessing as mp
import pandas as pd

from chimerapy.core.process import Process
from chimerapy.core.queue import PortableQueue

# Internal Imports
# Logging
import logging
logger = logging.getLogger(__name__)


class DataStream(Process):
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
        super().__init__(
            name=self.__class__.__name__,
            inputs=None,
        )
        # this makes sure that there is no inqueue in the data
        self.in_queue = None
        self.out_queue = PortableQueue(maxsize=10)

        self.name = name
        self.index = 0
        self.before_trim_time = None
        self.after_trim_time = None
        # initialize with a negative value first.
        # the collector/user of data_strem must run startup with appropriate time_window
        self.current_window = mp.Value("i", 0)
        self.windows = None

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

    def __eq__(self, other: 'DataStream') -> bool:
        if type(other) != type(self):
            return False
        else:
            return self.name == other.name
        
    def __hash__(self):
        return hash("hashvaluefor-{self.name}")

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

    def startup(self, windows):
        """Start the data stream by applying the before and after trims."""

        # Assuming that the timetrack has been already created
        if isinstance(self.before_trim_time, pd.Timedelta):
            self.trim_before(self.before_trim_time)

        if isinstance(self.after_trim_time, pd.Timedelta):
            self.trim_after(self.after_trim_time)
        
        self.windows = windows
        logger.debug(f"DataStream: Starting ...")

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

    def get_start_end(self, *args, **kwargs):
        raise NotImplementedError

    def step(self, *args, **kwargs):
        # this method runs the process and every step in the process.
        # this is something that is implemented in the child classes
        if self.windows is None:
            raise NotImplementedError("self.window was not initialized. Check if setup was called")

        start, end = self.windows[self.current_window.value]
        with self.current_window.get_lock():
            self.current_window.value += 1
        
        logger.debug(f"{self.__class__.__name__}: {len(self.windows)}, {self.current_window.value}: Run")

        data = self.get_start_end(start, end)
        return data

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
