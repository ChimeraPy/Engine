"""Module focused on the ``Collector`` and its various implementations.

Contains the following classes:
    ``Collector``

"""

# Package Management
__package__ = 'pymmdt'

# Built-in Imports
from typing import Sequence, Dict, Optional, Any
import threading
import math
import collections
import time
import queue

# Third-party Imports
import tqdm
import pandas as pd

# Internal Imports
from .data_stream import DataStream
from .tools import threaded

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
            data_streams_groups:Dict[str, Sequence[DataStream]],
            time_window_size:pd.Timedelta,
            max_queue_size:Optional[int]=3,
            verbose:Optional[bool]=False
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
        self.time_window_size = time_window_size
        self.verbose = verbose

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
        
        # Create a queue used by the process to stored loaded data
        self.loading_queue = queue.Queue(maxsize=max_queue_size)
        
        # Create a process for loading data and storing in a Queue
        self.loading_thread = self.load_data_to_queue()
        self._thread_exit = threading.Event()
        self._thread_exit.clear() # Set as False in the beginning

    def set_start_time(self, time:pd.Timedelta):
        self.start_time = time

    def set_end_time(self, time:pd.Timedelta):
        self.end_time = time

    def start(self):

        # Determine how many time windows given the total time and size
        total_time = (self.end_time - self.start_time)
        num_of_windows = math.ceil(total_time / self.time_window_size)

        # Create unique namedtuple and storage for the Windows
        Window = collections.namedtuple("Window", ['start', 'end'])
        self.windows = []

        # For all the possible windows, calculate their start and end
        for x in range(num_of_windows):
            start = self.start_time + x * self.time_window_size 
            end = self.start_time + (x+1) * self.time_window_size
            capped_end = min(self.end_time, end)
            window = Window(start, capped_end)
            self.windows.append(window)

        # Start the loading data process
        self.loading_thread.start()

    def close(self):
        # Stop the loading process!
        self._thread_exit.set()
        self.loading_thread.join()

    @threaded
    def load_data_to_queue(self):
       
        # Continuously load data
        for window in tqdm.tqdm(self.windows, disable=not self.verbose, desc="Loading data"):

            # If exit requested, end process
            if self._thread_exit.is_set():
                break

            # Extract the start and end time from window
            start, end = window.start, window.end 

            # Get the data
            data = self.get(start, end)

            # Place the data in the queue
            while True:
                # Instead of just getting stuck, we need to check
                # if the process is supposed to stop.
                # If not, keep trying to put the data into the queue
                if self._thread_exit.is_set():
                    break
                
                elif self.loading_queue.maxsize != self.loading_queue.qsize():
                    # Once the frame is placed, exit to get the new frame
                    self.loading_queue.put(data.copy(), block=True)
                    break
                
                else:
                    time.sleep(0.1)
                    continue

        # Once all the data is over, send the message that the work is 
        # complete
        self.loading_queue.put("END", block=False)

    def get(self, start_time: pd.Timedelta, end_time: pd.Timedelta) -> Dict[str, Dict[str, pd.DataFrame]]:
        # Obtain the data samples from all data streams given the 
        # window start and end time
        all_samples = collections.defaultdict(dict)
        for group_name, ds_list in self.data_streams_groups.items():
            for ds in ds_list:
                all_samples[group_name][ds.name] = ds.get(start_time, end_time)

        return all_samples
