from typing import List, Dict, Sequence
import multiprocessing as mp
import threading
import collections
import queue
import time
import sys

# Third-party imports
from PIL import Image
import numpy as np
import tqdm
import pandas as pd

# PyMMDT Library
import pymmdt as mm
import pymmdt.tabular as mmt
import pymmdt.video as mmv

# Resource:
# https://stackoverflow.com/questions/8489684/python-subclassing-multiprocessing-process

class CommunicatingProcess(mp.Process):

    def __init__(
            self, 
            message_to_queue,
            message_from_queue
        ):
        super().__init__()

        # Storing the message queues
        self.message_to_queue = message_to_queue
        self.message_from_queue = message_from_queue

        # Create a mapping to messages and their respective functions
        self.message_to_functions = {
            'END': self.end
        }
        self.subclass_message_to_functions = {} # Overriden in subclass

    def setup(self):

        # Closing information
        self.thread_exit = threading.Event()
        self.thread_exit.clear()

        # Create the thread that checks messages
        self.check_messages_thread = self.check_messages()
        self.check_messages_thread.start()
    
    @mm.tools.threaded 
    def check_messages(self):

        # Set the flag to check if new message
        new_message = False
        message = None
        
        # Constantly check for messages
        while not self.thread_exit.is_set():
            
            # Prevent blocking, as we need to check if the thread_exist
            # is set.
            while not self.thread_exit.is_set():
                try:
                    message = self.message_to_queue.get(timeout=1)
                    new_message = True
                    break
                except queue.Empty:
                    new_message = False
                    time.sleep(0.1)
                    
            # Only process if new message is set and message exists
            if new_message and message:

                # Handle the incoming message
                # print(f"NEW MESSAGE - {message}")

                # META --> General
                if message['header'] == 'META':

                    # Determine the type of message and interpret it
                    func = self.message_to_functions[message['body']['type']]

                # else --> Specific on the process
                else:

                    # Determing the type of message by the subclasses's 
                    # mapping
                    func = self.subclass_message_to_functions[message['body']['type']]

                # After obtaining the function, execute it
                func()

    def end(self):

        # Setting the thread exit event
        self.thread_exit.set()

    def close(self):

        # Clearing the messaging queues
        for queue in [self.message_to_queue, self.message_from_queue]:
            mm.tools.clear_queue(queue)

        # Waiting for the thread to end
        self.check_messages_thread.join()

        # Closing appropriately
        super().close()

class DataLoadingProcess(CommunicatingProcess):

    def __init__(
            self, 
            logdir, 
            loading_data_queue, 
            message_to_queue,
            message_from_queue,
            windows, 
            time_window, 
            unique_users, 
            users_meta, 
            verbose=False
        ):
        super().__init__(
            message_to_queue=message_to_queue,
            message_from_queue=message_from_queue
        )

        # Save the input parameters
        self.logdir = logdir
        self.loading_data_queue = loading_data_queue
        self.windows = windows
        self.time_window = time_window
        self.unique_users = unique_users
        self.users_meta = users_meta
        self.verbose = verbose
         
    def load_data_streams(self, unique_users:List, users_meta:Dict) -> Dict[str, Sequence[mm.DataStream]]:

        # Loading data streams
        users_data_streams = collections.defaultdict(list)
        for user_name, user_meta in tqdm.tqdm(zip(unique_users, users_meta), disable=not self.verbose):
            for index, row in user_meta.iterrows():

                # Extract useful meta
                entry_name = row['entry_name']
                dtype = row['dtype']

                # Construct the directory the file/directory is found
                if row['is_subsession']:
                    file_dir = self.logdir / row['user']
                else:
                    file_dir = self.logdir
                
                # Load the data
                if dtype == 'video':
                    ds = mmv.VideoDataStream(
                        name=entry_name,
                        start_time=row['start_time'],
                        video_path=file_dir/f"{entry_name}.avi"
                    )
                elif dtype == 'image':

                    # Load the meta CSV
                    df = pd.read_csv(file_dir/entry_name/'timestamps.csv')

                    # Load all the images into a data frame
                    img_filepaths = []
                    for index, row in df.iterrows():
                        img_fp = file_dir / entry_name / f"{row['idx']}.jpg"
                        # print(img_fp)
                        img_filepaths.append(img_fp)
                        # imgs.append(mm.tools.to_numpy(Image.open(img_fp)))
                    df['img_filepaths'] = img_filepaths
                    # df.to_csv('images_test.csv')
                    
                    # Create ds
                    ds = mmt.TabularDataStream(
                        name=entry_name,
                        data=df,
                        time_column='_time_'
                    )
                elif dtype == 'tabular':
                    raise NotImplementedError("Tabular visualization is still not implemented.")
                else:
                    raise RuntimeError(f"{dtype} is not a valid option.")

                # Store the data in Dict
                users_data_streams[user_name].append(ds)

        # Return the Dict[str, List[DataStream]]
        return users_data_streams

    def message_collector_construction(self):

        # Create the message
        collector_construction_message = {
            'header': 'META',
            'body': {
                'type': 'INIT',
                'content': {}
            }
        }

        # Send the message
        self.message_from_queue.put(collector_construction_message)

    def message_loading_window_counter(self):

        # Create the message
        loading_window_message = {
            'header': 'UPDATE',
            'body': {
                'type': 'COUNTER',
                'content': {
                    'loading_window': self.loading_window
                }
            }
        }

        # Send the message
        self.message_from_queue.put(loading_window_message)

    def message_finished_loading(self):

        # Create the message
        finished_loading_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {}
            }
        }

        # Sending the message
        self.message_from_queue.put(finished_loading_message)

        # Also tell the sorting process that the data is complete
        self.loading_data_queue.put('END')

    def run(self):

        # Perform process setup
        self.setup()

        # Load the data streams and create the Collector
        users_data_streams = self.load_data_streams(self.unique_users, self.users_meta)
        self.collector = mm.Collector.empty()
        self.collector.set_data_streams(users_data_streams, self.time_window)

        # Set the initial value
        self.loading_window = 0 

        print("Number of windows", len(self.windows))

        # Get the data continously
        while not self.thread_exit.is_set():

            # Check if the loading is halted
            if self.loading_window == -1:
                time.sleep(0.5)

            # Only load windows if there are more to load
            elif self.loading_window < len(self.windows):

                # Get the window information of the window to load to queue
                window = self.windows[int(self.loading_window)]

                # Extract the start and end time from window
                start, end = window.start, window.end 

                # Get the data
                data = self.collector.get(start, end)
                timetrack = self.collector.get_timetrack(start, end)

                data_chunk = {
                    'window_idx': self.loading_window,
                    # 'start': start,
                    # 'end': end,
                    'data': data,
                    'timetrack': timetrack
                }

                # Put the data into the queue
                while not self.thread_exit.is_set():
                    try:
                        self.loading_data_queue.put(data_chunk.copy(), timeout=1)
                        break
                    except queue.Full:
                        time.sleep(0.1)

                # Update the loading window pointer
                self.loading_window += 1
                self.message_loading_window_counter()

            # If finished, then tell the manager!
            elif self.loading_window == len(self.windows):

                # Sending message about finishing loading images
                self.message_finished_loading()

                # Setting the loading window to another value
                self.loading_window = -1
       
        # Closing process
        self.close()

class DataSortingProcess(CommunicatingProcess):

    def __init__(self,
            loading_data_queue,
            sorted_data_queue,
            message_to_queue,
            message_from_queue,
            entries,
            verbose=False
        ):
        
        super().__init__(
            message_to_queue=message_to_queue,
            message_from_queue=message_from_queue
        )

        # Saving the input variables
        self.loading_data_queue = loading_data_queue
        self.sorted_data_queue = sorted_data_queue
        self.verbose = verbose

        # Keeping track of the entries and their meta information
        self.entries = entries.set_index(['user', 'entry_name'])

    def message_loading_sorted(self, entry_time):

        # Create the message
        loading_window_message = {
            'header': 'UPDATE',
            'body': {
                'type': 'COUNTER',
                'content': {
                    'loaded_time': entry_time
                }
            }
        }

        # Send the message
        self.message_from_queue.put(loading_window_message)

    def message_finished_sorting(self):

        # Create the message
        finished_sorting_message = {
            'header': 'UPDATE',
            'body': {
                'type': 'END',
                'content': {}
            }
        }

        # Send the message
        self.message_from_queue.put(finished_sorting_message)

    def add_content_to_queue(self, entry):

        # Obtaining the data type
        # print(entry.group, entry.ds_type)
        entry_dtype = self.entries.loc[entry.group, entry.ds_type]['dtype']

        # Extract the content for each type of data stream
        if entry_dtype == 'image':
            # print(entry.img_filepaths)
            content = mm.tools.to_numpy(Image.open(entry.img_filepaths))
        elif entry_dtype == 'video':
            content = entry.frames
        else:
            raise NotImplementedError(f"{entry_dtype} content is not implemented yet!")

        # Creating the data chunk
        data_chunk = {
            'index': entry.name,
            'user': entry.group,
            'entry_time': entry._time_,
            'entry_name': entry.ds_type,
            'content': content
        }

        # But the entry into the queue
        while not self.thread_exit.is_set():
            try:
                # Putting the data
                self.sorted_data_queue.put(data_chunk.copy(), timeout=1)
                break
            except queue.Full:
                time.sleep(0.1)
            
        # Only update sorting bar entry other 10 samples
        if entry.name % 50 == 0:
            self.message_loading_sorted(entry._time_)

    def run(self):

        # Run the setup for the process
        self.setup()
 
        # Continously update the content
        while not self.thread_exit.is_set():

            # Get the next window of information if done processing 
            # the previous window data
            try:
                data_chunk = self.loading_data_queue.get(timeout=1)
            except queue.Empty:
                time.sleep(0.1)
                continue
            
            # If 'END' message, continue
            if isinstance(data_chunk, str):
                self.message_finished_sorting()
                continue
            
            # Decomposing the window item
            current_window_idx = data_chunk['window_idx']
            current_window_data = data_chunk['data']
            current_window_timetrack = data_chunk['timetrack']

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
