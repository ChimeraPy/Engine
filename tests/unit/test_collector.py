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

class CollectorTestCase(unittest.TestCase):
    
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
            start_time=pd.Timedelta(0),
            video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
        )
        self.dss = [self.tabular_ds, self.video_ds]

        # Create a collector 
        self.collector = cp.Collector(
            {'P01': self.dss},
            time_window=pd.Timedelta(seconds=2),
        )

        return None

    def test_empty_collector(self):

        # Test empty collector constructor
        empty_collector = cp.Collector(empty=True)
        empty_collector.set_data_streams({'P01': self.dss}, pd.Timedelta(seconds=2))
        
        # Setting start and end time
        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=1)

        # Then check that its the same
        data1 = self.collector.get(start_time, end_time)
        data2 = empty_collector.get(start_time, end_time)

        # The output should be the same
        for user in data1.keys():
            for ds_name in data1[user].keys():
                assert data1[user][ds_name].equals(data2[user][ds_name])

    def test_getting_data_from_all_data_streams(self):

        # NOTE! Slow test because video comparing!

        # Setting start and end time
        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=1)

        # Get data
        data = self.collector.get(start_time, end_time)

        # Ensure that the data makes sense
        assert isinstance(data, dict)
        
        # Comparing with the expected data
        tabular_test_data = self.tabular_ds.get(start_time, end_time)
        video_test_data = self.video_ds.get(start_time, end_time)
        
        assert data['P01']['test_tabular'].equals(tabular_test_data)
        assert data['P01']['test_video'].equals(video_test_data)

        return None

if __name__ == '__main__':
    # Run when debugging is not needed
    unittest.main()

    # Otherwise, we have to call the tests ourselves
    # test_case = CollectorTestCase()
    # test_case.setUp()
    # test_case.test_get()
