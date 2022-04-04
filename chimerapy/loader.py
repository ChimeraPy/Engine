from typing import Dict, Sequence, Optional, Any
import multiprocessing as mp
import queue
import time
import uuid

# Third-party imports
import pandas as pd

# ChimeraPy Library
from .core.tools import get_memory_data_size, PortableQueue
from .core.data_stream import DataStream
from .core.collector import Collector
from .base_process import BaseProcess

class Loader(BaseProcess):
    """Subprocess tasked with loading data from memory.

    This process plays the vital role of loading the data from memory.
    The class creates an instance of the ``Collector`` that aligns all
    the input datastreams in ``user_data_streams``. All the loaded data
    is put into ``loading_queue`` for other processes to use.

    """

    def __init__(
            self,
            loading_queue:PortableQueue,
            message_to_queue:PortableQueue,
            message_from_queue:PortableQueue,
            users_data_streams:Dict[str, Sequence[DataStream]],
            time_window:pd.Timedelta=pd.Timedelta(seconds=3),
            start_time:Optional[pd.Timedelta]=None,
            end_time:Optional[pd.Timedelta]=None,
            verbose:bool=False
        ):
        """Construct a ``Loader`` obtain to load data from data streams.

        Args:
            loading_queue (PortableQueue): The queue where loaded items are \
                placed.
            
            message_to_queue (PortableQueue): The queue used for sending \
                messages to the ``Loader`` process.
            
            message_from_queue (PortableQueue): The queue used for receiving \
                messages from the ``Loader`` process.

            users_data_streams (Dict[str, Sequence[DataStream]]): The \
                datastreams to be synchronized and loaded from by the \
                ``Collector`` instance.

            time_window (pd.Timedelta): The size of the time window used \
                for loading data by the ``Collector``.

            start_time (pd.Timedelta): The initial start time of the \
                global timetrack. This is to help skip parts that are not \
                so important.

            end_time (pd.Timedelta): The end time to restrict all data \
                stream timelines when construct the global timetrack.

            verbose (bool): Enabling debugging printouts.

        """
        super().__init__(
            message_to_queue=message_to_queue,
            message_from_queue=message_from_queue,
            verbose=verbose
        )

        # Save the input parameters
        self.loading_queue = loading_queue
        self.users_data_streams = users_data_streams
        self.time_window = time_window
        self.start_time = start_time
        self.end_time = end_time
    
        # Adding specific function class from the message
        self.subclass_message_to_functions.update({
            'LOADING_WINDOW': self.set_loading_window
        })

    def set_loading_window(self, loading_window:int):
        """message_to function to set the loading window.

        Args:
            loading_window (int): loading_window

        """
        # Set the time window
        self.loading_window = loading_window
         
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
            print("Timetrack update message failed to send!")

    def message_loading_window_counter(self, data_chunk: Dict[str, Any]):
        """message_from function to provide memory usage of data chunk.

        Args:
            data_chunk (Dict[str, Any]): Data chunk being placed in the \
                queue, which its memory consumption needs to be tracked.

        """
        # Create the message
        loading_window_message = {
            'header': 'UPDATE',
            'body': {
                'type': 'COUNTER',
                'content': {
                    'uuid': data_chunk['uuid'],
                    'loading_window': self.loading_window,
                    'data_memory_usage': get_memory_data_size(data_chunk)
                }
            }
        }

        # Send the message
        try:
            self.message_from_queue.put(loading_window_message.copy(), timeout=0.5)
        except queue.Full:
            print("Loading window counter message failed to send!")

    def message_finished_loading(self):
        """message_from function to inform that the loading is complete."""
        # Create the message
        finished_loading_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {}
            }
        }

        # Sending the message
        try:
            self.message_from_queue.put(finished_loading_message.copy(), timeout=0.5)
        except queue.Full:
            print("Finished loading messaged failed to send!")

        # Also tell the sorting process that the data is complete
        try:
            self.loading_queue.put('END', timeout=0.5)
        except queue.Full:
            print("END message failed to send!")

    def run(self):
        """Run the subprocess to load the data.

        This is the main routine of the ``Loader``. It loads data from 
        all data streams in time window intervals. The ``Collector``
        instance handles the alignment of the data streams by
        constructing the global timeline. Once we get a chunk of data,
        the ``Loader`` places it into the ``loading_queue`` for other
        subprocesses to ``get`` from.

        """
        # Perform process setup
        self.setup()

        # Load the data streams and create the Collector
        self.collector = Collector(
            data_streams_groups=self.users_data_streams,
            time_window=self.time_window,
            start_time=self.start_time,
            end_time=self.end_time,
            verbose=self.verbose,
        )

        # Get information about the collector's windows
        self.windows = self.collector.windows

        # Sending the global timetrack to the Manager
        self.message_timetrack_update()

        # Set the initial value
        self.loading_window = 0 

        # Get the data continously
        while not self.thread_exit.is_set():

            # Check if the loading is halted
            if self.loading_window == -1 or self.thread_pause.is_set():
                if self.verbose:
                    print(f"LOADER WAITING: {self.loading_window} or {self.thread_pause.is_set()}")
                time.sleep(0.5)

            # Only load windows if there are more to load
            elif self.loading_window < len(self.windows):

                # Get the window information of the window to load to queue
                window = self.windows[int(self.loading_window)]

                # Extract the start and end time from window
                start, end = window.start, window.end 

                # Get the data
                data = self.collector.get(start, end)
                data_is_loaded = False

                # Creating data chunk with uuid
                data_chunk = {
                    'uuid': uuid.uuid4(),
                    'data': data,
                }

                # Put the data into the queue
                while not self.thread_exit.is_set():
                    try:
                        self.loading_queue.put(data_chunk, timeout=0.1)
                        data_is_loaded = True
                        break
                    except queue.Full:
                        time.sleep(0.1)

                # Update the loading window pointer
                if data_is_loaded:
                    self.loading_window += 1
                    self.message_loading_window_counter(data_chunk)

            # If finished, then tell the manager!
            elif self.loading_window >= len(self.windows):

                # Sending message about finishing loading images
                self.message_finished_loading()

                # Setting the loading window to another value
                self.loading_window = -1

        # Closing the collector
        self.collector.close()
       
        # Closing process
        self.close()
