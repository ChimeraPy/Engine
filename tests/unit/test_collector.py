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
import pymmdt as mm
import pymmdt.tabular as mmt
import pymmdt.video as mmv

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
        self.tabular_ds = mmt.TabularDataStream(
            name="test_tabular",
            data=self.csv_data,
            time_column="_time_"
        )
        self.video_ds = mmv.VideoDataStream(
            name="test_video",
            start_time=pd.Timedelta(0),
            video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
        )
        self.dss = [self.tabular_ds, self.video_ds]

        # Storing collector parameters
        self.memory_limit = 0.1

        # Create a collector 
        self.collector = mm.Collector(
            {'P01': self.dss},
            time_window_size=pd.Timedelta(seconds=2),
            memory_limit=self.memory_limit
        )

        return None

    def test_empty_collector(self):

        # Test empty collector constructor
        empty_collector = mm.Collector(empty=True)
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

    def test_simple_get_memory_leakage(self):
        
        # Setting start and end time
        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=1)

        # Get the memory before
        pre_free = mm.tools.get_free_memory()

        # Get data
        data = self.collector.get(start_time, end_time)

        # Now delete it and see if the data is gone
        del data
        gc.collect()

        # Then get the memory after
        post_free = mm.tools.get_free_memory()

        # The pre and post should be close
        diff = np.abs(post_free - pre_free) / pre_free

        # This difference should be minimal
        assert diff < 0.1, f"Memory Leak of {diff}!"
    
    def test_collector_memory_limit(self):

        # Setting start and end time
        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=2)

        # Creating container of data
        all_datas = []
        all_memory = 0

        # Take snapshot of memory before
        pre_free = mm.tools.get_free_memory()

        # Check that the collector does exceed memory limit
        while True:

            try:
                data = self.collector.get(start_time, end_time)
            except mm.MemoryLimitError:
                break

            # Append data to keep it in memory (leakage)
            all_datas.append(data)
            per_sample_memory = 0
            for key, value in data.items():
                for k, v in value.items():
                    per_sample_memory += v.memory_usage(deep=True).sum()

            # Adding the per_sample_memory to total
            all_memory += per_sample_memory

        # Reporting number of blocks
        print(f"Total Number of loaded datas: {len(all_datas)}")

        # Then delete the memory
        del all_datas
        gc.collect()

        # Take snapshot of memory and ensure the available memory reflects
        # the log data is removed.
        post_free = mm.tools.get_free_memory()

        # Calculate the diff of memory
        diff = np.abs(post_free - pre_free) / pre_free

        # This difference should be minimal
        assert diff < 0.1, f"Memory Leak of {diff}!"

    def test_loading_thread(self):
        
        # Need to add the queue externally
        loading_queue = queue.Queue(maxsize=1000)
        self.collector.set_loading_queue(loading_queue)
        
        # Starting the collector thread
        thread = self.collector.load_data_to_queue()
        thread.start()

        # Clearing out the loading data queue
        data_counter = 0
        while True:

            time.sleep(0.01)

            if self.collector.loading_queue.qsize() != 0:
                data = self.collector.loading_queue.get()

                if data == 'END':
                    break
                else:
                    data_counter += 1

                # Delete memory
                del data
                gc.collect()
                print("Deleting and collecting!")
       
        # Joining thread later
        thread.join()

        # Now we need to ensure that the number of elements in the queue
        # equals the number of windows
        assert data_counter == len(self.collector.windows)

    def test_shortening_start_and_end(self):
        
        # Setting start and end time
        start_time = pd.Timedelta(seconds=2)
        end_time = pd.Timedelta(seconds=10)

        # Now testing if we shorten the windows!
        self.collector.set_start_time(start_time)
        self.collector.set_end_time(end_time)
        
        # Need to add the queue externally
        loading_queue = queue.Queue(maxsize=1000)
        self.collector.set_loading_queue(loading_queue)
        
        # Starting the collector thread
        thread = self.collector.load_data_to_queue()
        thread.start()
        
        # Clearing out the loading data queue
        data_counter = 0
        while True:

            time.sleep(0.01)

            if self.collector.loading_queue.qsize() != 0:
                data = self.collector.loading_queue.get()

                if data == 'END':
                    break
                else:
                    data_counter += 1

                # Delete memory
                del data
                gc.collect()
                print("Deleting and collecting!")
        
        # Joining thread later
        thread.join()

        # Now we need to ensure that the number of elements in the queue
        # equals the number of windows
        assert data_counter == len(self.collector.windows)
 
    def test_stress_the_memory_limit_with_slow_unloading(self):
        
        # Need to add the queue externally
        loading_queue = queue.Queue(maxsize=1000)
        self.collector.set_loading_queue(loading_queue)
        
        # Starting the collector thread
        thread = self.collector.load_data_to_queue()
        thread.start()

        # Clearing out the loading data queue
        data_counter = 0
        while True:

            time.sleep(0.5)

            if self.collector.loading_queue.qsize() != 0:
                data = self.collector.loading_queue.get()

                if data == 'END':
                    break
                else:
                    data_counter += 1

                # Delete memory
                del data
                gc.collect()
                print("Deleting and collecting!")
        
        # Joining thread later
        print("JOINING!")
        thread.join()

        # Now we need to ensure that the number of elements in the queue
        # equals the number of windows
        assert data_counter == len(self.collector.windows)

if __name__ == '__main__':
    # Run when debugging is not needed
    unittest.main()

    # Otherwise, we have to call the tests ourselves
    # test_case = CollectorTestCase()
    # test_case.setUp()
    # test_case.test_get()
