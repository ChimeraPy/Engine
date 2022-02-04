from typing import List, Dict, Sequence
import multiprocessing as mp
import threading
import collections
import queue
import time

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
                        img_filepaths.append(img_fp)
                        # imgs.append(mm.tools.to_numpy(Image.open(img_fp)))
                    df['img_filepaths'] = img_filepaths
                    
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
        # self.entries_dict = self.entries.to_dict()
        # print(self.entries_dict)
        # print(self.entries)

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
            # current_window_start = data_chunk['start']
            # current_window_end = data_chunk['end'] 

            # Adding a column tracking which rows have been used
            for user, entries in current_window_data.items():
                for entry_name, entry_data in entries.items():
                    used = [False for x in range(len(entry_data))]
                    current_window_data[user][entry_name].loc[:,'used'] = used

            for index, row in current_window_timetrack.iterrows():

                # Exit early if thread_exit is called!
                if self.thread_exit.is_set():
                    break

                # Extract row and entry information
                user = row.group
                entry_name = row.ds_type
                entry_time = row.time
                entry_data = current_window_data[user][entry_name]
                entry_dtype = self.entries.loc[user, entry_name]['dtype']

                # If the entry is not empty
                if not entry_data.empty:

                    # Get the entries that have not been used
                    new_samples_loc = (entry_data['used'] == False)

                    # If there are remaining entries, use them!
                    if new_samples_loc.any():

                        # Get the entries idx
                        latest_content_idx = np.where(new_samples_loc)[0][0] 
                        latest_content = entry_data.iloc[latest_content_idx]

                        # If image, load the image and then place it in
                        # the queue.
                        if entry_dtype == 'image':
                            latest_content['images'] = mm.tools.to_numpy(Image.open(latest_content['img_filepaths']))

                        # Creating the data chunk
                        data_chunk = {
                            'index': index,
                            'user': user,
                            'entry_time': entry_time,
                            'entry_name': entry_name,
                            'content': latest_content
                        }

                        # But the entry into the queue
                        while not self.thread_exit.is_set():
                            try:
                                # Putting the data
                                self.sorted_data_queue.put(data_chunk.copy(), timeout=1)

                                break
                            except queue.Full:
                                time.sleep(0.1)
                                
                        # If successful, then mark it
                        entry_data.at[latest_content_idx, 'used'] = True

                        # Only update sorting bar entry other 10 samples
                        if index % 50 == 0:
                            self.message_loading_sorted(entry_time)

        # Closing the process
        self.close()
