# Built-in Imports
import unittest
import pathlib
import shutil
import os
import sys
import time
import collections
import queue

# Third-Party Imports
import tqdm
import pandas as pd

# Testing Library
import pymmdt as mm
import pymmdt.tabular as mmt
import pymmdt.video as mmv

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = CURRENT_DIR / 'data' 
OUTPUT_DIR = CURRENT_DIR / 'test_output' 

sys.path.append(str(ROOT_DIR))

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
            fps=30
        )

        # Create a collector 
        self.collector = mm.Collector(
            {'P01': [self.tabular_ds, self.video_ds]},
            time_window_size=pd.Timedelta(seconds=2),
            verbose=True
        )

        return None

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

    def test_loading_thread(self):
        
        # Need to add the queue externally
        loading_queue = queue.Queue(maxsize=1000)
        self.collector.set_loading_queue(loading_queue)
        
        # Starting the collector thread
        thread = self.collector.load_data_to_queue()
        thread.start()
        thread.join()

        # Now we need to ensure that the number of elements in the queue
        # equals the number of windows
        # The +1 is the closing "END" message
        assert self.collector.loading_queue.qsize() == len(self.collector.windows) + 1

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
        thread.join()

        # Now we need to ensure that the number of elements in the queue
        # equals the number of windows
        # The +1 is the closing "END" message
        assert self.collector.loading_queue.qsize() == len(self.collector.windows) + 1

if __name__ == '__main__':
    # Run when debugging is not needed
    unittest.main()

    # Otherwise, we have to call the tests ourselves
    # test_case = CollectorTestCase()
    # test_case.setUp()
    # test_case.test_get()
