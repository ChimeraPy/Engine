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
# from memory_profiler import profile
import numpy as np
import pandas as pd

# Testing Library
import chimerapy as cp

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

class SessionTestCase(unittest.TestCase):

    def setUp(self):

        # Storing the data
        csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
        csv_data['_time_'] = pd.to_timedelta(csv_data['time'], unit="s")

        # Create each type of data stream
        self.tabular_ds = cp.TabularDataStream(
            name="test_tabular",
            data=csv_data,
            time_column="_time_"
        )
        self.video_ds = cp.VideoDataStream(
            name="test_video",
            start_time=pd.Timedelta(0),
            video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
            startup_now=True
        )
        
        # Create a list of the data streams
        self.dss = [self.tabular_ds, self.video_ds]
        
        # Clear out the previous ChimeraPy run 
        # since pipeline is still underdevelopment
        self.exp_dir = OUTPUT_DIR / "ChimeraPy"
        if self.exp_dir.exists():
            shutil.rmtree(self.exp_dir)
        
        # Create the logging queue and exiting event
        self.logging_queue = cp.tools.PortableQueue(maxsize=100)
        # self.logging_queue = queue.Queue(maxsize=100)
        self.thread_exit = threading.Event()
        self.thread_exit.clear()

        # Construct the individual participant pipeline object
        # Create an overall session and pipeline
        self.session = cp.Session(
            name='test',
            logging_queue=self.logging_queue
        )

    def adding_test_data(self):
        
        # Timing
        start_time = pd.Timedelta(seconds=0)
        tab_end_time = pd.Timedelta(seconds=10)
        video_end_time = pd.Timedelta(seconds=1)
        
        test_tabular_data = self.tabular_ds.get(start_time, tab_end_time)
        test_video_data = self.video_ds.get(start_time, video_end_time)

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

    def test_single_session_logging_data(self):
        # Loading the data
        self.adding_test_data()
        
        # Check the number of log data matches the queue
        assert self.logging_queue.qsize() == 4

        # Clear the queue
        cp.tools.clear_queue(self.logging_queue)
  
if __name__ == "__main__":
    # unittest.main()

    test_case = SessionTestCase()
    test_case.setUp()
    test_case.test_memory_management()
