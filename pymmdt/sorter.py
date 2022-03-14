from typing import List, Dict, Sequence
import multiprocessing as mp
import queue
import time
import uuid

# Third-party imports
from PIL import Image
import numpy as np
import tqdm
import pandas as pd

# PyMMDT Library
from .core.tools import to_numpy, get_memory_data_size
from .base_process import BaseProcess

# Resource:
# https://stackoverflow.com/questions/8489684/python-subclassing-multiprocessing-process

class Sorter(BaseProcess):

    def __init__(self,
            loading_queue:mp.Queue,
            sorting_queue:mp.Queue,
            message_to_queue:mp.Queue,
            message_from_queue:mp.Queue,
            entries:pd.DataFrame,
            update_counter_period:int=10,
            verbose=False
        ):
        
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

    def message_sorter_pulled_from_loading_queue(self, uuid):

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

    def message_loading_sorted(self, data_chunk):

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

    def add_content_to_queue(self, entry):

        # Obtaining the data type
        entry_dtype = self.entries.loc[entry.group, entry.ds_type]['dtype']

        # Extract the content for each type of data stream
        if entry_dtype == 'image':
            content = to_numpy(Image.open(entry.img_filepaths))
        elif entry_dtype == 'video':
            content = entry.frames
        else:
            raise NotImplementedError(f"{entry_dtype} content is not implemented yet!")

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
                continue
            
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
            # for index, row in total_df.iterrows():

            #     # Check if we need to stop ASAP
            #     if self.thread_exit.is_set():
            #         break

            #     # Update content
            #     self.add_content_to_queue(row)

        # Closing the process
        self.close()
