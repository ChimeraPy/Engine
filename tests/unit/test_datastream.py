# Built-in Imports
import gc
import unittest
import pathlib
import shutil
import os
import sys
import time
import collections
import queue

# Third-Party Imports
import cv2
import numpy as np
import tqdm
import pandas as pd

# Testing Library
import chimerapy as cp

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

class DataStreamTestCase(unittest.TestCase):

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
            fps=30,
            startup_now=True
        )

        # Create empty version of the data streams
        self.empty_tabular_ds = cp.TabularDataStream.empty(name="test_empty_tabular")
        self.empty_video_ds = cp.VideoDataStream.empty(
            name="test_empty_video",
            start_time=pd.Timedelta(0),
            fps=30,
            size=self.video_ds.get_frame_size(),
            startup_now=True
        )

        # Creating list container for all datastreams
        self.dss = [self.tabular_ds, self.video_ds]

        return None

    def test_getting_data_once(self):

        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=0.1)

        video_data = self.video_ds.get(start_time, end_time)
        tabular_data = self.tabular_ds.get(start_time, end_time)

        return None

    def test_getting_windowed_data(self):

        # Get the latest timetrack value
        timetrack_ends = [ds.timetrack.iloc[-1].time for ds in self.dss]
        latest_timetrack_end = max(timetrack_ends)
        end = latest_timetrack_end.seconds

        # Testing all types of data streams
        step = 5
        for ds in self.dss:
            for start, end in tqdm.tqdm(zip(range(0,end,step), range(step,end,step))):
                data = ds.get(pd.Timedelta(seconds=start), pd.Timedelta(seconds=end))
                
                # Testing here
                assert isinstance(data, pd.DataFrame)
                if data.empty != True: # only if not empty
                    assert '_time_' in data.keys()

        return None

    def test_appending_data(self):

        # Getting data to append
        csv_append_data = self.tabular_ds.get(
            start_time=pd.Timedelta(seconds=0),
            end_time=pd.Timedelta(seconds=5)
        )

        # Appending data
        self.tabular_ds.append(csv_append_data)

        return None

    def test_empty_data_stream_and_filling(self):

        # Defining start and end time
        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=5)
       
        # Obtaining data to append to empty tabular datastream
        csv_append_data = self.tabular_ds.get(start_time,end_time)
        self.empty_tabular_ds.append(csv_append_data)
        
        # Ensuring that the data is saved correctly
        saved_appended_data = self.empty_tabular_ds.get(start_time, end_time)
        assert csv_append_data.equals(saved_appended_data)

        # Obtaining data to append to empty video datastream
        video_append_data = self.video_ds.get(start_time,end_time)
        self.empty_video_ds.append(video_append_data)

        return None
    
    def test_generating_data_stream_from_another_and_a_process(self):
        
        # TODO: Implement this feature for other types of data streams:
        # VideoDataStream

        new_tabular_ds = cp.TabularDataStream.from_process_and_ds(
            name='test_new_tabular',
            process=cp.IdentityProcess(),
            in_ds=self.tabular_ds
        )

        # Now they should be equal
        assert new_tabular_ds == self.tabular_ds 

        return None

    def test_tricping_before(self):
        
        # Defining start and end time
        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=5)

        for ds in self.dss:

            # Get data before the trim
            before_trim_data = ds.get(start_time, end_time)
            assert before_trim_data.empty != True

            # Then apply tricping
            ds.trim_before(end_time)

            # Get the data after tricping
            after_trim_data = ds.get(start_time, end_time)
            assert before_trim_data.equals(after_trim_data) != True
            assert after_trim_data.empty

        return None

    def test_tricping_after(self):
        
        # Defining start and end time
        start_time = pd.Timedelta(seconds=10)
        end_time = pd.Timedelta(seconds=15)

        for ds in self.dss:

            # Get data before the trim
            before_trim_data = ds.get(start_time, end_time)
            assert before_trim_data.empty != True

            # Then apply tricping
            ds.trim_after(start_time)

            # Get the data after tricping
            after_trim_data = ds.get(start_time, end_time)
            assert before_trim_data.equals(after_trim_data) != True
            assert after_trim_data.empty

        return None

    def test_closing_data_streams(self):

        # This should close the data stream without issues
        for ds in self.dss:
            ds.close()

        for ds in [self.empty_tabular_ds, self.empty_video_ds]:
            ds.close()

if __name__ == "__main__":
    unittest.main()

    # For debugging purposes
    # test = DataStreamTestCase()
    # test.setUp()
    # test.test_tricping_before()
