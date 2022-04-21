# Built-in Imports
import json
import time
import unittest
import pathlib
import shutil
import os
import multiprocessing as mp
import queue

# Third-Party Imports
# from memory_profiler import profile
import pprint
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

class LoggerTests(unittest.TestCase):

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
        
        # Creating the parameters to the logger
        self.logdir = OUTPUT_DIR
        self.experiment_name = 'testing_logger'
        
        # Clear out the previous run
        experiment_dir = self.logdir / self.experiment_name
        if experiment_dir.exists():
            shutil.rmtree(experiment_dir)
            os.mkdir(experiment_dir)

        self.logging_queue = cp.tools.PortableQueue(maxsize=100)
        self.message_to_logger = cp.tools.PortableQueue(maxsize=100)
        self.message_from_logger = cp.tools.PortableQueue(maxsize=100)
      
        # Create the logger
        self.logger = cp.Logger(
            logdir=self.logdir,
            experiment_name='testing_logger',
            logging_queue=self.logging_queue,
            message_to_queue=self.message_to_logger,
            message_from_queue=self.message_from_logger,
            verbose=True
        )
    
    def adding_test_data(self, session):
        
        # Timing
        start_time = pd.Timedelta(seconds=0)
        tab_end_time = pd.Timedelta(seconds=10)
        video_end_time = pd.Timedelta(seconds=0.1)
        
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
 
        # Create root and other sessions that interface to the logger
        root_session = cp.Session(
            name='root',
            logging_queue=self.logging_queue
        )
        subsession = cp.Session(
            name='P01',
            logging_queue=self.logging_queue
        )

        print("Finished session construction")

        # Add data
        self.adding_test_data(root_session)
        self.adding_test_data(subsession)

        # Check how much data chunks were passed to the logger
        total_logged_data = self.logging_queue.qsize()
        print(f'Total logged data: {total_logged_data}')
        
        # Start the logger
        self.logger.start()

        print("Started logger")

        # Wait until the logger is done
        while self.logging_queue.qsize() != 0:
            time.sleep(0.1)
        
        print("Finished logging data")

        # Then tell logger to stop!
        end_message = {
            'header': 'META',
            'body': {
                'type': 'END',
                'content': {},
            }
        }
        self.message_to_logger.put(end_message)

        print("Sent END message")

        # Wait until the logger confirms shutdown
        messages = []
        while True:
            try:
                # Read the message
                message = self.message_from_logger.get(timeout=0.1)
                messages.append(message)

                # Break condition
                if message['body']['type'] == 'END':
                    break

            except queue.Empty:
                print(".", end="")
                time.sleep(0.1)

        # Printout the messages
        pprint.pprint(messages)
       
        # Then wait until the loader joins
        print(self.logging_queue.qsize(), self.message_from_logger.qsize(), self.message_to_logger.qsize())
        self.logger.join()

        print("Logger joined!")

        # Now we have to check that the logger did the right behavior
        assert self.logging_queue.qsize() == 0, f"logging queue should be empty, instead it is {self.logging_queue.qsize()}"
        assert len(messages) == total_logged_data + 1, f"For each logged data, we should receive logging confirmation from the Logger (exp: {total_logged_data+1}, actual: {len(messages)})."


    # @profile
    def test_logger_memory_stress(self):

        # Create root and other sessions that interface to the logger
        root_session = cp.Session(
            name='root',
            logging_queue=self.logging_queue
        )
        subsession = cp.Session(
            name='P01',
            logging_queue=self.logging_queue
        )

        print("Finished session construction")

        # Add data
        for i in range(3):
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
        self.message_to_logger.put(end_message)

        time.sleep(0.5)

        print("Sent END message")

        # Then get the messages from the logger
        messages = []
        while self.message_from_logger.qsize() != 0:
            messages.append(self.message_from_logger.get())

        print(messages)
  
        # Then wait until the logger joins
        print(self.logging_queue.qsize(), self.message_from_logger.qsize(), self.message_to_logger.qsize())
        self.logger.join()

        print("Logger joined!")

    def test_single_session_threading_saving_and_closing(self):

        # Create root and other sessions that interface to the logger
        root_session = cp.Session(
            name='root',
            logging_queue=self.logging_queue
        )

        print("Finished session construction")

        # Add data
        for i in range(3):
            self.adding_test_data(root_session)

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
        self.message_to_logger.put(end_message)

        print("Sent END message")

        time.sleep(1)

        # Then get the messages from the logger
        messages = []
        while self.message_from_logger.qsize() != 0:
            messages.append(self.message_from_logger.get())
  
        # Then wait until the logger joins
        print(self.logging_queue.qsize(), self.message_from_logger.qsize(), self.message_to_logger.qsize())
        self.logger.join()

        # All the data should be processed
        assert self.logging_queue.qsize() == 0

        # Check about the generated meta file
        expected_meta = {
            'id': 'root',
            'records': {
                'root': {
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
                }
            },
            'subsessions': []
        }
        with open(self.logger.experiment_dir / "meta.json", "r") as json_file:
            actual_meta = json.load(json_file)

        print("EXPECTED META")
        pprint.pprint(expected_meta)
        print(2*"\n")

        print("ACTUAL META")
        pprint.pprint(actual_meta)

if __name__ == "__main__":
    unittest.main()

    # test_case = LoggerTests()
    # test_case.setUp()
    # test_case.test_logger_memory_stress()
