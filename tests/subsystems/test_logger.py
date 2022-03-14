# Built-in Imports
import queue
import time
import unittest
import pathlib
import shutil
import os
import multiprocessing as mp

# Third-Party Imports
from memory_profiler import profile
import pprint
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

class LoggerTests(unittest.TestCase):

    def setUp(self):

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
        
        # Creating the parameters to the logger
        self.logdir = OUTPUT_DIR
        self.experiment_name = 'testing_logger'
        
        # Clear out the previous run
        experiment_dir = self.logdir / self.experiment_name
        if experiment_dir.exists():
            shutil.rmtree(experiment_dir)
            os.mkdir(experiment_dir)

        self.logging_queue = mp.Queue(maxsize=100)
        self.message_to_queue = mp.Queue(maxsize=100)
        self.message_from_queue = mp.Queue(maxsize=100)
        # self.logging_queue = queue.Queue(maxsize=100)
        # self.message_to_queue = queue.Queue(maxsize=100)
        # self.message_from_queue = queue.Queue(maxsize=100)
      
        # Create the logger
        self.logger = mm.Logger(
            logdir=self.logdir,
            experiment_name='testing_logger',
            logging_queue=self.logging_queue,
            message_to_queue=self.message_to_queue,
            message_from_queue=self.message_from_queue,
            verbose=True
        )
    
    def adding_test_data(self, session):
        
        # Timing
        start_time = pd.Timedelta(seconds=0)
        tab_end_time = pd.Timedelta(seconds=10)
        video_end_time = pd.Timedelta(seconds=1)
        
        test_tabular_data = self.tabular_ds.get(start_time, tab_end_time)
        test_video_data = self.video_ds.get(start_time, video_end_time)

        # Test all types of logging
        session.add_tabular(
            name='test_tabular',
            data=test_tabular_data,
            time_column='_time_'
        )
        session.add_image(
            name='test_image_with_timestamp',
            data=test_video_data['frames'].iloc[0],
            timestamp=test_video_data['_time_'].iloc[0]
        )
        session.add_images(
            name='test_images',
            df=test_video_data,
            data_column='frames',
            time_column='_time_'
        )
        session.add_video(
            name='test_video',
            df=test_video_data,
        )

    def test_creating_and_simple_running_logger(self):
 
        # Start the logger
        self.logger.start()

        print("Started logger")

        # Create root and other sessions that interface to the logger
        root_session = mm.core.Session(
            name='root',
            logging_queue=self.logging_queue
        )
        subsession = mm.core.Session(
            name='P01',
            logging_queue=self.logging_queue
        )

        print("Finished session construction")

        # Add data
        self.adding_test_data(root_session)
        self.adding_test_data(subsession)
        
        print("Finished logging data")

        # Then tell logger to stop!
        end_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {},
            }
        }
        self.message_to_queue.put(end_message)

        print("Sent END message")
       
        # Then wait until the loader joins
        self.logger.join()

        print("Logger joined!")
        
        # Then get the messages from the loader
        messages = []
        while self.message_from_queue.qsize() != 0:
            messages.append(self.message_from_queue.get())

        print(messages)

    @profile
    def test_logger_memory_stress(self):

        # Create root and other sessions that interface to the logger
        root_session = mm.core.Session(
            name='root',
            logging_queue=self.logging_queue
        )
        subsession = mm.core.Session(
            name='P01',
            logging_queue=self.logging_queue
        )

        print("Finished session construction")

        # Add data
        for i in range(10):
            self.adding_test_data(root_session)
            self.adding_test_data(subsession)

        time.sleep(3)
        
        print("Finished logging data")
        
        # Start the logger
        self.logger.start()

        print("Started logger")
        
        # Then tell logger to stop!
        end_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {},
            }
        }
        self.message_to_queue.put(end_message)

        print("Sent END message")
  
        # Then wait until the loader joins
        self.logger.join()
        
        # Then get the messages from the loader
        messages = []
        while self.message_from_queue.qsize() != 0:
            messages.append(self.message_from_queue.get())

        print(messages)

        print("Logger joined!")

    def test_single_session_threading_saving_and_closing(self):

        # Load the data into the queue
        self.adding_test_data()
        self.adding_test_data()

        # Create the thread
        thread = self.session.load_data_to_log()

        # Start the thread and tell it to quit
        thread.start()
        self.thread_exit.set()
        thread.join()

        # All the data should be processed
        assert self.logging_queue.qsize() == 0

        # Closing session to ensure that everything 
        self.session.close()

        # First, the files should exists now
        for entry in self.session.records.values():
            assert entry.save_loc.exists()

        # The estimated FPS should be close to the input FPS
        estimated_fps = self.session.records['test_video'].stream.fps
        actual_fps = self.video_ds.fps
        assert estimated_fps == actual_fps, \
            f"Estimate FPS: {estimated_fps} vs. Actual FPS: {actual_fps}"

        # Check about the generated meta file
        expected_meta = {
            'id': 'pymmdt',
            'records': {
                'test_tabular': {
                    'dtype': 'tabular',
                    'start_time': str(pd.Timedelta(0)),
                    'end_time': str(pd.Timedelta(seconds=9))
                },
                'test_image_with_timestamp': {
                    'dtype': 'image',
                    'start_time': str(pd.Timedelta(0)),
                    'end_time': str(pd.Timedelta(0)),
                },
                'test_images': {
                    'dtype': 'image',
                    'start_time': str(pd.Timedelta(0)),
                    'end_time': str(pd.Timedelta(seconds=0.482758612)),
                },
                'test_video': {
                    'dtype': 'video',
                    'start_time': str(pd.Timedelta(0)),
                    'end_time': str(pd.Timedelta(seconds=0.482758612)),
                }
            },
            'subsessions': []
        }
        with open(self.session.session_dir / "meta.json", "r") as json_file:
            actual_meta = json.load(json_file)

        # Convert all the str pd.Timedelta to actual
        for record_name in actual_meta['records'].keys():
            start_time = actual_meta['records'][record_name]['start_time']
            end_time = actual_meta['records'][record_name]['end_time']
            # actual_meta['records'][record_name]['start_time'] = pd.to_timedelta(start_time)
            # actual_meta['records'][record_name]['end_time'] = pd.to_timedelta(end_time)
            actual_meta['records'][record_name]['start_time'] = start_time
            actual_meta['records'][record_name]['end_time'] = end_time

        pprint.pprint(expected_meta)
        pprint.pprint(actual_meta)

if __name__ == "__main__":
    # unittest.main()

    test_case = LoggerTests()
    test_case.setUp()
    test_case.test_logger_memory_stress()
