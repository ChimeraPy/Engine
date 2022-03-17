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
import pymmdt as mm
import pymmdt.core.tabular as mmt
import pymmdt.core.video as mmv

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

class LoaderTests(unittest.TestCase):
    
    def setUp(self):

        # Storing the data
        self.csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
        self.csv_data['_time_'] = pd.to_timedelta(self.csv_data['time'], unit="s")

        # Create each type of data stream
        self.tabular_ds = mmt.TabularDataStream(
            name="test_tabular",
            data=self.csv_data,
            time_column="_time_"
        )
        self.video_ds = mmv.VideoDataStream(
            name="test_video",
            start_time=pd.Timedelta(seconds=0),
            video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
        )
        self.dss = {'P01': [self.tabular_ds, self.video_ds]}

        # Storing collector parameters
        self.memory_limit = 0.1

        return None

    def test_creating_and_simple_running_loader(self):
        
        # Creating the necessary queues
        delay = 1 # has to be at least 0.2
        q_max_size = 3
        loading_queue = mp.Queue(maxsize=q_max_size)
        message_to_queue = mp.Queue(maxsize=100)
        message_from_queue = mp.Queue(maxsize=100)
        time_window = pd.Timedelta(seconds=0.1)

        # Create data loader
        loader = mm.Loader(
            loading_queue=loading_queue,
            message_to_queue=message_to_queue,
            message_from_queue=message_from_queue,
            users_data_streams=self.dss,
            time_window=time_window,
            verbose=True
        )

        # Start the loader and get data
        loader.start()

        # Wait until the data queue is full
        time.sleep(delay)

        # Then tell the loader to stop!
        end_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {},
            }
        }
        message_to_queue.put(end_message)
        
        # Wait until loader fully stops
        time.sleep(delay)

        # Then get the data
        datas = []
        while loading_queue.qsize() != 0:
            data = loading_queue.get()
            datas.append(data)

        # Then verify the data
        assert len(datas) == q_max_size, f"{len(datas)} != {q_max_size}"

        # Then get the messages from the loader
        messages = []
        while message_from_queue.qsize() != 0:
            messages.append(message_from_queue.get())
       
        # Verify that messages were sent (+1 for initialization)
        assert len(messages) == q_max_size + 1, f"{len(messages)} != {q_max_size+1}"
        
        # Then wait until the loader joins
        loader.join()

if __name__ == '__main__':
    unittest.main()
