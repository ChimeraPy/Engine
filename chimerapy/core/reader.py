# Package Management
__package__ = 'chimerapy'

# Built-in Imports
from typing import Sequence, Dict, Optional, Any
import collections
import queue
import multiprocessing as mp
import uuid
import signal
import logging

# Internal Imports
from chimerapy.core.process import Process
from chimerapy.core.queue import PortableQueue
from chimerapy.core.data_stream import DataStream
from chimerapy.utils.tools import get_windows
from chimerapy.utils.memory_manager import MemoryManager

# Third-party imports
import pandas as pd

# Logging
logger = logging.getLogger(__name__)


class Reader(Process):
    """Subprocess tasked with reading data from memory.

    This process plays the vital role of reading the data from memory.
    The class creates an instance of the ``Collector`` that aligns all
    the input datastreams in ``user_data_streams``. All the readed data
    is put into ``reading_queue`` for other processes to use.

    """

    def __init__(
            self,
            data_streams:Dict[str, DataStream],
            memory_manager:MemoryManager,
            time_window:pd.Timedelta=pd.Timedelta(seconds=3),
            start_time:Optional[pd.Timedelta]=None,
            end_time:Optional[pd.Timedelta]=None,
        ):
        """Construct a ``Reader`` obtain to read data from data streams.

        Args:

            data_streams (Dict[str, DataStream]): The \
                datastreams to be synchronized and readed from by the \
                ``Collector`` instance.

            time_window (pd.Timedelta): The size of the time window used \
                for reading data by the ``Collector``.

            start_time (pd.Timedelta): The initial start time of the \
                global timetrack. This is to help skip parts that are not \
                so important.

            end_time (pd.Timedelta): The end time to restrict all data \
                stream timelines when construct the global timetrack.

        """
        super().__init__(
            name=self.__class__.__name__,
            inputs=None,
        )
        # Constructing the data stream dictionary
        self.data_streams = data_streams
        self.time_window = time_window
        self.memory_manager = memory_manager
        self.time_window = time_window
        self.start_time = start_time
        self.end_time = end_time

        self.windows = []
        # Set the initial value
        self.window_index = mp.Value('i', 0)

        # Adding specific function class from the message
        self.subclass_message_to_functions.update({
            'WINDOW_INDEX': self.set_window_index
        })

    def __len__(self):
        """Get the size of the global timetrack."""
        return len(self.global_timetrack)

    @classmethod
    def empty(cls):
        """Construct an empty ``Collector``."""
        return cls(empty=True)

    def set_window_index(self, window_index:int):
        """message_to function to set the window index.

        Args:
            window_index (int): window_index

        """
        # Set the time window
        with self.window_index.get_lock:
            self.window_index.value = window_index


    def message_timetrack_update(self):
        """message_from function to provide updated timetrack info."""
        # Create the message
        collector_construction_message = {
            'header': 'UPDATE',
            'body': {
                'type': 'TIMETRACK',
                'content': {
                    'timetrack': self.global_timetrack.copy(),
                    'windows': self.windows
                }
            }
        }
        # Send the message
        try:
            self.message_out_queue.put(collector_construction_message.copy(), timeout=0.5)
        except queue.Full:
            logger.debug("Timetrack update message failed to send!")


    def message_finished_reading(self):
        """message_from function to inform that the reading is complete."""
        # Create the message
        finished_reading_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {}
            }
        }

        # Sending the message
        try:
            self.message_out_queue.put(finished_reading_message.copy(), timeout=0.5)
        except queue.Full:
            logger.debug("Finished reading messaged failed to send!")

        # Also tell the sorting process that the data is complete
        try:
            self.out_queue.put('END', timeout=0.5)
        except queue.Full:
            logger.debug("END message failed to send!")

    def setup(self) -> None:

        # Starting up the datastreams
        logger.debug(f"Collector: Starting datasets")
        for ds in self.data_streams.values():
            ds.startup(self.windows)

        # Construct a global timetrack
        self.construct_global_timetrack()
        # Apply triming if start_time or end_time has been selected
        if type(self.start_time) != type(None) and isinstance(self.start_time, pd.Timedelta):
            self.set_start_time(self.start_time)
        if type(self.end_time) != type(None) and isinstance(self.end_time, pd.Timedelta):
            self.set_end_time(self.end_time)
    
        # Determine the number of windows
        self.windows = get_windows(self.start_time, self.end_time, self.time_window)
        
        # start the data_stream fetches
        for ds in self.data_streams.values():
            ds.start()

    def construct_global_timetrack(self):
        """Construct the global timetrack.

        The method for constructing the global timetrack is by using the
        timelines of each data streams. A pd.TimedeltaIndex column 
        with labelled ``time`` is used to sort the multimodal data. 

        """
        dss_times= []
        for name, ds in self.data_streams.items():
            # Obtaining each ds's timetrack and adding an ds_type 
            # identifier to know which data stream
            time_series = ds.timetrack.copy()
            time_series['group'] = name
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

    def get(self, timeout:float=None):
        # this calls the self.step() function to get the data_chunk
        data_chunk = super().get(timeout)
        if data_chunk:
            self.memory_manager.remove(data_chunk)
        return data_chunk

    def step(self) -> Dict[str, Dict[str, pd.DataFrame]]:
        """Getting data from all data streams.

        Obtain the data samples from all data streams given the 
        window start and end time. Additionally, we are tracking the 
        average memory consumed per-group samples. 

        """

        # Before anything, check that there is enough memory
        logger.debug(f"Reader: {self.memory_manager.total_memory_used()}")
        if self.memory_manager.total_memory_used(percentage=True) > 1:
            logger.info("Reader: consumed all memory")
            return None

        # Iterating over all groups (like users) and their corresponding
        # data streams.
        any_empty = any(ds.out_queue.qsize() <= 0 for ds in self.data_streams.values())
        if any_empty:
            return False

        all_samples = collections.defaultdict(pd.DataFrame)
        for ds in self.data_streams.values():
            # Obtaining the sample and storing it
            sample: pd.DataFrame = ds.get(timeout=0.1)
            all_samples[ds.name] = sample

        data_chunk = {
            'uuid': str(uuid.uuid4()),
            'data': all_samples,
            'window_index': self.window_index.value
        }

        self.memory_manager.add(data_chunk, which='reader')
        with self.window_index.get_lock():
            self.window_index.value += 1

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

    def shutdown(self):
        # Prepare the DataStreams by startup them!
        for ds in self.data_streams.values():
            ds.close()
            ds.shutdown()

        super().shutdown()

