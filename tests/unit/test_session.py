# Built-in Imports
import multiprocessing as mp
import gc
import time
import unittest
import threading
import pathlib
import shutil
import os
import queue
import json
import pprint

# Third-Party Imports
import psutil
from memory_profiler import profile
import numpy as np
import pandas as pd

# Testing Library
import pymmdt as mm
import pymmdt.core.tabular as mmt
import pymmdt.core.video as mmv
from pymmdt.core.tools import get_logging_data_size

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

class SessionTestCase(unittest.TestCase):

    def setUp(self):

        self.initial_used = psutil.virtual_memory().used
        print('Initial used: ', self.initial_used)

        # Storing the data
        csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
        csv_data['_time_'] = pd.to_timedelta(csv_data['time'], unit="s")

        # Create each type of data stream
        self.tabular_ds = mmt.TabularDataStream(
            name="test_tabular",
            data=csv_data,
            time_column="_time_"
        )
        self.video_ds = mmv.VideoDataStream(
            name="test_video",
            start_time=pd.Timedelta(0),
            video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
            startup_now=True
        )
        
        # Create a list of the data streams
        self.dss = [self.tabular_ds, self.video_ds]
        
        # Clear out the previous pymmdt run 
        # since pipeline is still underdevelopment
        self.exp_dir = OUTPUT_DIR / "pymmdt"
        if self.exp_dir.exists():
            shutil.rmtree(self.exp_dir)
        
        # Create the logging queue and exiting event
        self.logging_queue = mp.Queue(maxsize=100)
        # self.logging_queue = queue.Queue(maxsize=100)
        self.thread_exit = threading.Event()
        self.thread_exit.clear()

        # Construct the individual participant pipeline object
        # Create an overall session and pipeline
        self.session = mm.core.Session(
            name='test',
            logging_queue=self.logging_queue
        )

        # # Create a collector 
        # self.memory_limit = 0.8
        # self.collector = mm.core.Collector(
        #     {'P01': self.dss},
        #     time_window=pd.Timedelta(seconds=2),
        #     memory_limit=self.memory_limit
        # )

    def adding_test_data(self):
        
        # Timing
        start_time = pd.Timedelta(seconds=0)
        tab_end_time = pd.Timedelta(seconds=10)
        video_end_time = pd.Timedelta(seconds=1)
        
        test_tabular_data = self.tabular_ds.get(start_time, tab_end_time)
        test_video_data = self.video_ds.get(start_time, video_end_time)

        # Get memory used
        memory_usage = test_tabular_data.memory_usage(deep=True).sum()
        memory_usage += 2*test_video_data.memory_usage(deep=True).sum()

        # Test all types of logging
        self.session.add_tabular(
            name='test_tabular',
            data=test_tabular_data,
            time_column='_time_'
        )
        self.session.add_image(
            name='test_image_with_timestamp',
            data=test_video_data['frames'].iloc[0],
            timestamp=test_video_data['_time_'].iloc[0]
        )
        self.session.add_images(
            name='test_images',
            df=test_video_data,
            data_column='frames',
            time_column='_time_'
        )
        self.session.add_video(
            name='test_video',
            df=test_video_data,
        )

        return memory_usage

    def test_logging_memory(self):
        
        # Timing
        start_time = pd.Timedelta(seconds=0)
        tab_end_time = pd.Timedelta(seconds=10)
        video_end_time = pd.Timedelta(seconds=0.1)

        pre_used = psutil.virtual_memory().used - self.initial_used

        for i in range(10):
        
            # test_tabular_data = self.tabular_ds.get(start_time, tab_end_time)
            test_video_data = self.video_ds.get(start_time, video_end_time)

            # Get the memory of the pd.DataFrame
            # pre_tab_data_memory = test_tabular_data.memory_usage(deep=True).sum()
            pre_video_data_memory = test_video_data.memory_usage(deep=True).sum()

            # Putting in the queue
            # self.logging_queue.put(test_tabular_data)
            self.logging_queue.put(test_video_data)

            # Then pulling the data from the queue
            # tab_data = self.logging_queue.get()
            video_data = self.logging_queue.get()

            # # Ensure that the data is valid
            assert test_video_data.equals(video_data)
            # assert test_tabular_data.equals(tab_data)

            # Checking that the memory is reasonable
            # post_tab_data_memory = tab_data.memory_usage(deep=True).sum()
            post_video_data_memory = video_data.memory_usage(deep=True).sum()

            # print('Tab: ', pre_tab_data_memory, post_tab_data_memory)
            print('Video: ', pre_video_data_memory, post_video_data_memory)
            
            print(test_video_data.info(memory_usage="deep"))
            print(video_data.info(memory_usage='deep'))

            print(test_video_data['frames'][0].shape)
            print(video_data['frames'][0].shape)

            # del tab_data, video_data, test_tabular_data, test_video_data
            del video_data, test_video_data
            gc.collect()

            post_used = psutil.virtual_memory().used - self.initial_used
            print(pre_used, post_used, post_used/pre_used)
         
        # The memory should be the same
        # assert pre_video_data_memory == post_video_data_memory

    def test_single_session_logging_data(self):
        # Loading the data
        self.adding_test_data()
        
        # Check the number of log data matches the queue
        assert self.logging_queue.qsize() == 4

        # Clear the queue
        mm.core.tools.clear_queue(self.logging_queue)
  
    @profile
    def test_memory_management(self):

        # Loading data
        total_memory_usage = 0
        for i in range(20):
            total_memory_usage += self.adding_test_data()

        # Clear out the 
        while not self.logging_queue.qsize() == 0:
            print(self.logging_queue.qsize())
            data = self.logging_queue.get()
            data_memory_usage = get_logging_data_size(data)
            total_memory_usage -= data_memory_usage
            time.sleep(0.05)
            del data
            gc.collect()

        print(total_memory_usage)

if __name__ == "__main__":
    # unittest.main()

    test_case = SessionTestCase()
    test_case.setUp()
    test_case.test_memory_management()
