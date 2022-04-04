
# Built-in Imports
import unittest
import pathlib
import shutil
import os
import sys
import time
import collections
import queue
import gc
import multiprocessing as mp

# Third-Party Imports
import tqdm
import numpy as np
import psutil
import pandas as pd

# Testing Library
import chimerapy as cp

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

class SorterTests(unittest.TestCase):
    
    def setUp(self):

        # Storing the data
        self.csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
        self.csv_data['_time_'] = pd.to_timedelta(self.csv_data['time'], unit="s")

        # Create each type of data stream
        self.tabular_ds = cp.TabularDataStream(
            name="test_tabular",
            data=self.csv_data,
            time_column="_time_"
        )
        self.video_ds = cp.VideoDataStream(
            name="test_video",
            start_time=pd.Timedelta(seconds=0),
            video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
        )
        self.dss = {'root': [self.tabular_ds, self.video_ds]}
        self.entries = pd.DataFrame({'user': ['root', 'root'], 'entry_name': ['test_tabular', 'test_video'], 'dtype': ['tabular', 'video']})

        # Storing collector parameters
        self.memory_limit = 0.1

        return None

    def test_creating_and_simple_running_loader(self):
        
        # Creating the necessary queues
        delay = 2 # has to be at least 0.2 (for linux) and 2 (for macOS)
        q_max_size = 5
        time_window = pd.Timedelta(seconds=0.1)
        update_counter_period = 2

        loading_queue = cp.tools.PortableQueue(maxsize=q_max_size)
        message_loading_to_queue = cp.tools.PortableQueue(maxsize=100)
        message_loading_from_queue = cp.tools.PortableQueue(maxsize=100)

        sorting_queue = cp.tools.PortableQueue(maxsize=q_max_size*5)
        message_sorting_to_queue = cp.tools.PortableQueue(maxsize=100)
        message_sorting_from_queue = cp.tools.PortableQueue(maxsize=100)

        # Create data loader
        loader = cp.Loader(
            loading_queue=loading_queue,
            message_to_queue=message_loading_to_queue,
            message_from_queue=message_loading_from_queue,
            users_data_streams=self.dss,
            time_window=time_window,
            verbose=True
        )

        # Create data sorter
        sorter = cp.Sorter(
            loading_queue=loading_queue,
            sorting_queue=sorting_queue,
            message_to_queue=message_sorting_to_queue,
            message_from_queue=message_sorting_from_queue,
            entries=self.entries,
            update_counter_period=update_counter_period,
            verbose=True
        )

        # Start the loader and get data
        loader.start()

        # Wait until the data queue is full
        while loading_queue.qsize() != q_max_size:
            time.sleep(0.1)

        # Then tell the loader to stop!
        end_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {},
            }
        }
        message_loading_to_queue.put(end_message)

        # Now start the sorter
        print("Start sorter")
        sorter.start()
        
        # Wait until sorter complex all
        while loading_queue.qsize() != 0:
            time.sleep(0.1)
        
        # Give some time for the sorter to finish all
        message_sorting_to_queue.put(end_message)

        # Get loader data
        l_datas = []
        while loading_queue.qsize() != 0:
            data = loading_queue.get()
            l_datas.append(data)
        print(len(l_datas))
        
        # Get loader data
        s_datas = []
        while sorting_queue.qsize() != 0:
            data = sorting_queue.get()
            s_datas.append(data)
        print(len(s_datas))

        # Then get the messages from the loader
        l_messages = []
        while message_loading_from_queue.qsize() != 0:
            l_messages.append(message_loading_from_queue.get())
        print(len(l_messages))
        
        # Then get the messages from the loader
        s_messages = []
        while message_sorting_from_queue.qsize() != 0:
            s_messages.append(message_sorting_from_queue.get())
        print(len(s_messages))

        assert len(l_datas) == 0
        assert len(l_messages)-1 == q_max_size

        # Then wait until the loader and sorter joins
        loader.join()
        sorter.join()

if __name__ == '__main__':
    unittest.main()
