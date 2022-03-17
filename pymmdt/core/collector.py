"""Module focused on the ``Collector`` and its various implementations.

Contains the following classes:
    ``Collector``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Sequence, Dict
import os
import time
import psutil
import collections
import queue
import gc

# Third-party Imports
import pandas as pd

# Internal Imports
from pymmdt.core.data_stream import DataStream
from pymmdt.core.tools import threaded, get_windows

class Collector:
    """Generic collector that stores only data streams.

    The offline collector allows the use of both __getitem__ and __next__
    to obtain the data pointer to a data stream to fetch the actual data.

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
        return cls(empty=True)

    def set_data_streams(
            self, 
            data_streams_groups:Dict[str, Sequence[DataStream]]={},
            time_window:pd.Timedelta=pd.Timedelta(0),
        ) -> None:

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
        # self.global_timetrack.to_csv('global_timetrack.csv', index=False)
        
    def set_start_time(self, time:pd.Timedelta):
        assert time < self.end_time, "start_time cannot be greater than end_time."
        self.start_time = time

    def set_end_time(self, time:pd.Timedelta):
        assert time > self.start_time, "end_time cannot be smaller than start_time."
        self.end_time = time

    def set_loading_queue(self, loading_queue:queue.Queue):
        assert isinstance(loading_queue, queue.Queue), "loading_queue must be a queue.Queue."
        self.loading_queue = loading_queue

    def get(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> Dict[str, Dict[str, pd.DataFrame]]:
        """

        Obtain the data samples from all data streams given the 
        window start and end time. Additionally, we are tracking the 
        average memory consumed per-group samples. To avoid overloading,
        a memory limit is being placed. If the memory limit would be 
        exceeded, we raise a MemoryLimitError.

        Raises:
            MemoryLimitError: Collector `get` is trying to loading past 
            the memory limit.

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

    def get_timetrack(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> pd.DataFrame:
        return self.global_timetrack[(self.global_timetrack['time'] >= start_time) & (self.global_timetrack['time'] < end_time)]

    def __len__(self):
        return len(self.global_timetrack)

    def close(self):

        for dss in self.data_streams_groups.values():
            for ds in dss:
                ds.close()
