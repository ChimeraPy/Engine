from typing import Dict, Sequence, Optional, Any
import multiprocessing as mp
import queue
import time
import uuid
import signal
import logging

# Third-party imports
import pandas as pd

# ChimeraPy Library
from chimerapy.utils.tools import PortableQueue
from chimerapy.utils.memory_manager import MemoryManager
from chimerapy.core.data_stream import DataStream
from chimerapy.core.collector import Collector
from chimerapy.core.process import Process
from chimerapy.utils.tools import clear_queue

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
            users_data_streams:Dict[str, Sequence[DataStream]],
            memory_manager:MemoryManager,
            time_window:pd.Timedelta=pd.Timedelta(seconds=3),
            start_time:Optional[pd.Timedelta]=None,
            end_time:Optional[pd.Timedelta]=None,
        ):
        """Construct a ``Reader`` obtain to read data from data streams.

        Args:

            users_data_streams (Dict[str, Sequence[DataStream]]): The \
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
            run_type='proactive'
        )

        # Save the input parameters
        self.users_data_streams = users_data_streams
        self.memory_manager = memory_manager
        self.time_window = time_window
        self.start_time = start_time
        self.end_time = end_time

        # Set the initial value
        self.window_index = mp.Value('i', 0)
        self.collector = None
        
        # Adding specific function class from the message
        self.subclass_message_to_functions.update({
            'WINDOW_INDEX': self.set_window_index
        })

    def set_window_index(self, window_index:int):
        """message_to function to set the window index.

        Args:
            window_index (int): window_index

        """
        # Set the time window
        self.window_index.value = window_index
         
    def message_timetrack_update(self):
        """message_from function to provide updated timetrack info."""
        # Create the message
        collector_construction_message = {
            'header': 'UPDATE',
            'body': {
                'type': 'TIMETRACK',
                'content': {
                    'timetrack': self.collector.global_timetrack.copy(),
                    'windows': self.collector.windows
                }
            }
        }

        # Send the message
        try:
            self.message_from_queue.put(collector_construction_message.copy(), timeout=0.5)
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
            self.message_from_queue.put(finished_reading_message.copy(), timeout=0.5)
        except queue.Full:
            logger.debug("Finished reading messaged failed to send!")

        # Also tell the sorting process that the data is complete
        try:
            self.data_from_queue.put('END', timeout=0.5)
        except queue.Full:
            logger.debug("END message failed to send!")

    def setup(self) -> None:
        
        # Ignore SIGINT signal
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        
        # read the data streams and create the Collector
        self.collector = Collector(
            data_streams_groups=self.users_data_streams,
            time_window=self.time_window,
            start_time=self.start_time,
            end_time=self.end_time,
        )

        # Get information about the collector's windows
        self.windows = self.collector.windows

        # Sending the global timetrack to the Manager
        # self.message_timetrack_update()

    def step(self, index:int=None) -> Optional[Dict[str, Any]]:
        """Single step in the Reader loop where data is readed and put."""

        # Before anything, check that there is enough memory
        logger.debug(f"Reader: {self.memory_manager.total_memory_used()}")
        if self.memory_manager.total_memory_used(percentage=True) > 1:
            logger.debug("Reader: consumed all memory")
            return None

        # Get the window information of the window to read to queue
        if not index:
            window = self.windows[self.window_index.value]
        else:
            window = self.windows[index]

        # Extract the start and end time from window
        start, end = window.start, window.end 

        # Get the data
        data = self.collector.get(start, end)

        # Creating data chunk with uuid
        data_chunk = {
            'uuid': uuid.uuid4(),
            'data': data,
            'window_index': self.window_index.value
        }
        self.memory_manager.add(data_chunk, which='reader')
        return data_chunk

    def get(self, timeout:float=None):

        # Get the data chunk and record that in the memory manager
        data_chunk = super().get(timeout)
        self.memory_manager.remove(data_chunk)

        # Return it as well
        return data_chunk

    def teardown(self):
        super().teardown()
        
        # Closing the collector
        if type(self.collector) != type(None):
            self.collector.close()

