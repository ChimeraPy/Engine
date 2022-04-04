from typing import Dict, Any
import multiprocessing as mp
import queue
import time
import uuid

# Third-party imports
from PIL import Image
import pandas as pd

# ChimeraPy Library
from .core.tools import to_numpy, get_memory_data_size, PortableQueue
from .base_process import BaseProcess

# Resource:
# https://stackoverflow.com/questions/8489684/python-subclassing-multiprocessing-process

class Sorter(BaseProcess):
    """Subprocess tasked with sorting the content temporally.

    The ``Sorter`` obtains data loaded by the ``Loader`` and sorts all
    the data samples (from all the data streams) into a single queue. 
    This makes it easier to synchronize content and how it is updated
    in the application front-end.

    """

    def __init__(self,
            loading_queue:PortableQueue,
            sorting_queue:PortableQueue,
            message_to_queue:PortableQueue,
            message_from_queue:PortableQueue,
            entries:pd.DataFrame,
            update_counter_period:int=10,
            verbose:bool=False
        ):
        """Construct a ``Sorter`` to temporally sort all data.

        Args:
            loading_queue (PortableQueue): The input loading queue that the \
                sorter will ``get`` data from.

            sorting_queue (PortableQueue): The output sorting queue that the \
                sorter will ``put`` data to.

            message_to_queue (PortableQueue): The message queue to send \
                messages to the ``Sorter``.

            message_from_queue (PortableQueue): The message queue to recieve \
                messages from the ``Sorter``.

            entries (pd.DataFrame): A data frame that contains various \
                attributes of the entries.

            update_counter_period (int): This is the period in when the \
                ``Sorter`` reports sorted data. Too frequent and the system \
                will be overloaded by the messages. Too large periods and \
                the memory consumption of the ``Sorter`` won't be accurately \
                tracked.

            verbose (bool): Debugging printout.
        """
        
        super().__init__(
            message_to_queue=message_to_queue,
            message_from_queue=message_from_queue,
            verbose=verbose
        )

        # Saving the input variables
        self.update_counter_period = update_counter_period
        self.loading_queue = loading_queue
        self.sorting_queue = sorting_queue
        self.sorted_entries_processed = 0

        # Keeping track of the entries and their meta information
        self.entries = entries.set_index(['user', 'entry_name'])

    def message_sorter_pulled_from_loading_queue(self, uuid:str):
        """``Sorter`` sending message: getting data from loading queue.

        Args:
            uuid (str): The unique identifier for a group of sorted data.

        """

        message = {
            'header': 'UPDATE',
            'body': {
                'type': 'MEMORY',
                'content': {
                    'uuid': uuid
                }
            }
        }

        # Send the message
        try:
            self.message_from_queue.put(message, timeout=0.5)
        except queue.Full:
            print("Error: ``message_sorter_pulled_from_loading_queue`` failed to send!")

    def message_loading_sorted(self, data_chunk:Dict[str,Any]):
        """``Sorter`` sending message: putting data to sorting queue.

        Args:
            data_chunk (Dict[str,Any]): The data chunk that is being \
                ``put`` into sorting queue.

        """
        # Check that the uuid is not None
        assert data_chunk['uuid'] != None

        # Create the message
        message = {
            'header': 'UPDATE',
            'body': {
                'type': 'COUNTER',
                'content': {
                    'uuid': data_chunk['uuid'],
                    'loaded_time': data_chunk['entry_time'],
                    'data_memory_usage': self.unregistered_memory_usage
                }
            }
        }

        # Send the message
        try:
            self.message_from_queue.put(message, timeout=0.5)
        except queue.Full:
            print("Error: loading sorted messaged failed to send!")

    def message_finished_sorting(self):
        """``Sorter`` sending message: finishing sorting all data."""

        # Create the message
        finished_sorting_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {}
            }
        }

        # Send the message
        try:
            self.message_from_queue.put(finished_sorting_message, timeout=0.5)
        except queue.Full:
            print("Error: finished sorting message failed to send!")

    def add_content_to_queue(self, entry:pd.Series):
        """Apply function for pd.DataFrame to individually put into sorting queue.

        This is the main routine that handles the individual rows in the
        collection of all the data samples.

        Args:
            entry (pd.Series): The row of the data frame to be added \
                to the sorting queue.

        """
        # Obtaining the data type
        entry_dtype = self.entries.loc[entry.group, entry.ds_type]['dtype']

        # Extract the content for each type of data stream
        if entry_dtype == 'image':
            content = to_numpy(Image.open(entry.img_filepaths))
        elif entry_dtype == 'video':
            if len(entry.frames.shape) == 3:
                content = entry.frames[:,:,::-1]
            else:
                content = entry.frames
        else:
            # raise NotImplementedError(f"{entry_dtype} content is not implemented yet!")
            return None

        # If updating, the we need to generate uuid
        if self.sorted_entries_processed % self.update_counter_period == 0 and \
            self.sorted_entries_processed != 0:
            data_uuid = uuid.uuid4()
        else:
            data_uuid = None

        # Creating the data chunk
        data_chunk = {
            'uuid': data_uuid,
            'index': self.sorted_entries_processed,
            'user': entry.group,
            'entry_time': entry._time_,
            'entry_name': entry.ds_type,
            'content': content
        }

        # Put the entry into the queue
        while not self.thread_exit.is_set():
            try:
                # Putting the data
                self.sorting_queue.put(data_chunk.copy(), timeout=1)
                break
            except queue.Full:
                print("full")
                time.sleep(0.1)
            
        # Only update sorting bar entry other 10 samples
        if self.sorted_entries_processed % self.update_counter_period == 0 and \
            self.sorted_entries_processed != 0:
            self.message_loading_sorted(data_chunk)
            self.unregistered_memory_usage = 0
        else:
            self.unregistered_memory_usage += get_memory_data_size(data_chunk)
        
        # Updating the number of processed data chunks
        self.sorted_entries_processed += 1

    def run(self):
        """The main routine for sorting data.

        The ``run`` method setup the communication thread and the gets 
        data from the ``loading queue``, sorts it, and places the sorted
        content into the ``sorting queue``. The sorted content is then
        used by the front-end application to update images, videos, and 
        other content.

        """
        # Run the setup for the process
        self.setup()

        # Using variable to track memory of multiple elements in the queue
        self.unregistered_memory_usage = 0
 
        # Continously update the content
        while not self.thread_exit.is_set():

            # If paused, just continue sleeping
            if self.thread_pause.is_set():
                if self.verbose:
                    print("SORTER WAITING")
                time.sleep(0.5)
                continue

            # Get the next window of information if done processing 
            # the previous window data
            try:
                data_chunk = self.loading_queue.get(timeout=1)
            except queue.Empty:
                time.sleep(0.1)
                print(f"size of loading queue: {self.loading_queue.qsize()}")
                continue

            # Printing information about pulling
            if self.verbose:
                print("[Sorter]: Pulled data from loading_queue.")
            
            # If 'END' message, continue
            if isinstance(data_chunk, str):
                self.message_finished_sorting()
                continue
            else: # Else, informed the Manager that the memory can be restored!
                self.message_sorter_pulled_from_loading_queue(data_chunk['uuid'])
            
            # Decomposing the window item
            # current_window_idx = data_chunk['window_idx']
            current_window_data = data_chunk['data']
            # current_window_timetrack = data_chunk['timetrack']

            # Placing all the data frames within a list to later concat
            all_dfs = []
            for user, entries in current_window_data.items():
                for entry_name, entry_data in entries.items():
                    df = current_window_data[user][entry_name]
                    df['group'] = user
                    df['ds_type'] = entry_name
                    all_dfs.append(current_window_data[user][entry_name])

            # Combining and sorting data
            total_df = pd.concat(all_dfs, axis=0, ignore_index=True)
            total_df.sort_values(by="_time_", inplace=True)
            total_df.reset_index(inplace=True)
            total_df = total_df.drop(columns=['index'])

            # Apply to the total_df
            total_df.apply(lambda x: self.add_content_to_queue(x), axis=1)

        # Closing the process
        self.close()
