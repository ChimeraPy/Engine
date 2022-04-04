# Package Management
__package__ = 'chimerapy'

# Built-in Imports
from typing import Sequence, Dict
import collections
import queue

# Third-party Imports
import pandas as pd

# Internal Imports
from chimerapy.core.data_stream import DataStream
from chimerapy.core.tools import get_windows

class Collector:
    """Data ``Collector`` loads, syncs, and fuzes data streams.

    The ``Collector`` has the responsibility of constructing the global
    timetrack and pulling data from the data streams by using the 
    timetrack.

    Attributes:
        data_streams (Dict[str, mm.DataStream]): A dictionary of the 
        data streams that its keys are the name of the data streams.

        global_timetrack (pd.DataFrame): A data frame that stores the time,
        data stream type, and data pointers to allow the iteration over
        all samples in all data streams efficiently.

    """

    def __init__(
            self, 
            data_streams_groups:Dict[str, Sequence[DataStream]]={},
            time_window:pd.Timedelta=pd.Timedelta(0),
            start_time:pd.Timedelta=None,
            end_time:pd.Timedelta=None,
            empty:bool=False,
            verbose:bool=False
        ) -> None:
        """Construct the ``Collector``.

        In the constructor, the global timeline/timetrack is generated
        that connects all the data streams. This universal timetrack
        only contains the pointers to the individual data stream values.
        By using only the pointers, the global timetrack is concise, 
        small, and cheap to generate on the fly.

        Once the timetrack is generated, the timetrack can be iterated
        from the beginning to end. The data stream pointers help
        retrieve the correct data in an orderly fashion.

        """
        # Constructing the data stream dictionary
        self.data_streams_groups = data_streams_groups
        self.time_window = time_window
        self.verbose = verbose

        # Starting up the datastreams
        for dss in self.data_streams_groups.values():
            for ds in dss:
                ds.startup()
        
        # Keeping counter for the number of windows loaded
        self.windows = []
        self.windows_loaded = -1

        # Only when data streams provided should we start handling timetrack
        if not empty:

            # Construct a global timetrack
            self.construct_global_timetrack()

            # Apply triming if start_time or end_time has been selected
            if type(start_time) != type(None) and isinstance(start_time, pd.Timedelta):
                self.set_start_time(start_time)
            if type(end_time) != type(None) and isinstance(end_time, pd.Timedelta):
                self.set_end_time(end_time)
        
            # Determine the number of windows
            self.windows = get_windows(self.start_time, self.end_time, self.time_window)

    @classmethod
    def empty(cls):
        """Construct an empty ``Collector``."""
        return cls(empty=True)

    def set_data_streams(
            self, 
            data_streams_groups:Dict[str, Sequence[DataStream]]={},
            time_window:pd.Timedelta=pd.Timedelta(0),
        ) -> None:
        """Setting the data streams for an empty ``Collector``.

        Args:
            data_streams_groups (Dict[str, Sequence[DataStream]]): The 
            data streams organized by the group they belong to. The 
            division of the groups is mostly by the ``SingleRunner``
            and ``GroupRunner``'s names.

            time_window (pd.Timedelta): Size of the time window.

        """
        # Prepare the DataStreams by startup them!
        for group_name, dss in data_streams_groups.items():
            for ds in dss:
                ds.startup()
        
        # Once data streams are provided and time_window, we can 
        # finally setup the global timetrack
        self.data_streams_groups = data_streams_groups
        self.time_window = time_window
        self.construct_global_timetrack()
        
        # Determine the number of windows
        self.windows = get_windows(self.start_time, self.end_time, self.time_window)

    def construct_global_timetrack(self):
        """Construct the global timetrack.

        The method for constructing the global timetrack is by using the
        timelines of each data streams. A pd.TimedeltaIndex column 
        with labelled ``time`` is used to sort the multimodal data. 

        If verbose, the global timetrack is saved as well.

        """
        dss_times= []
        for group_name, ds_list in self.data_streams_groups.items():
            for ds in ds_list:

                # Obtaining each ds's timetrack and adding an ds_type 
                # identifier to know which data stream
                time_series = ds.timetrack.copy()
                time_series['group'] = group_name
                time_series['ds_type'] = ds.name
                time_series['ds_index'] = [x for x in range(len(time_series))]

                # Storing the dataframe with all the other streams
                dss_times.append(time_series)

        # Converging the data streams tags to a global timetrack
        self.global_timetrack: pd.DataFrame = pd.concat(dss_times, axis=0)

        # Ensuring that the ds_index column is an integer
        self.global_timetrack['ds_index'] = self.global_timetrack['ds_index'].astype(int)
        self.global_timetrack.sort_values(by='time', inplace=True)
        self.global_timetrack.reset_index(inplace=True)
        self.global_timetrack = self.global_timetrack.drop(columns=['index'])
        
        # Split samples based on the time window size
        self.start_time = self.global_timetrack['time'][0]
        self.end_time = self.global_timetrack['time'][len(self.global_timetrack)-1]

        # For debugging purposes, save the timetrack to csv to debug
        if self.verbose:
            self.global_timetrack.to_csv('global_timetrack.csv', index=False)
        
    def set_start_time(self, time:pd.Timedelta):
        """Set the start time, clipping previous time in the global timetrack.

        Args:
            time (pd.Timedelta): start time clipping.

        """
        assert time < self.end_time, "start_time cannot be greater than end_time."
        self.start_time = time

    def set_end_time(self, time:pd.Timedelta):
        """Set the end time, clipping after time in the global timetrack.

        Args:
            time (pd.Timedelta): end time clipping.

        """
        assert time > self.start_time, "end_time cannot be smaller than start_time."
        self.end_time = time

    def set_loading_queue(self, loading_queue:queue.Queue):
        """Setting the loading queue to the ``Collector``.

        Args:
            loading_queue (queue.Queue): The loading queue to put data.

        """
        assert isinstance(loading_queue, queue.Queue), "loading_queue must be a queue.Queue."
        self.loading_queue = loading_queue

    def get(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Getting data from all data streams.

        Obtain the data samples from all data streams given the 
        window start and end time. Additionally, we are tracking the 
        average memory consumed per-group samples. 

        """
        # Checking input logic and type
        assert start_time < end_time, "start_time must be earlier than end_time."

        # Creating containers for all sample's data and meta data.
        all_samples = collections.defaultdict(dict)

        # Iterating over all groups (like users) and their corresponding
        # data streams.
        for group_name, ds_list in self.data_streams_groups.items():
            for ds in ds_list:
                
                # Obtaining the sample and storing it
                sample: pd.DataFrame = ds.get(start_time, end_time)
                all_samples[group_name][ds.name] = sample

        return all_samples

    def get_timetrack(
            self, 
            start_time: pd.Timedelta, 
            end_time: pd.Timedelta
        ) -> pd.DataFrame:
        """Get the timetrack that ranges from start to end time.

        Args:
            start_time (pd.Timedelta): Start of time window.
            end_time (pd.Timedelta): End of time window.

        Returns:
            pd.DataFrame: The global timetrack from that range.

        """
        return self.global_timetrack[(self.global_timetrack['time'] >= start_time) & (self.global_timetrack['time'] < end_time)]

    def __len__(self):
        """Get the size of the global timetrack."""
        return len(self.global_timetrack)

    def close(self):
        """Close all data streams and ``Collector``."""

        # Iterate through all data streams and close them.
        for dss in self.data_streams_groups.values():
            for ds in dss:
                ds.close()
