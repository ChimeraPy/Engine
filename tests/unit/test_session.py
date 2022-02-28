# Built-in Imports
import gc
import unittest
import threading
import pathlib
import shutil
import os
import queue
import json
import pprint

# Third-Party Imports
import numpy as np
import pandas as pd
import tqdm

# Testing Library
import pymmdt as mm
import pymmdt.tabular as mmt
import pymmdt.video as mmv

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

class SingleSessionTestCase(unittest.TestCase):

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
        )
        
        # Create a list of the data streams
        self.dss = [self.tabular_ds, self.video_ds]
        
        # Clear out the previous pymmdt run 
        # since pipeline is still underdevelopment
        self.exp_dir = OUTPUT_DIR / "pymmdt"
        if self.exp_dir.exists():
            shutil.rmtree(self.exp_dir)

        # Construct the individual participant pipeline object
        # Create an overall session and pipeline
        self.session = mm.Session(
            log_dir = OUTPUT_DIR,
            experiment_name = "pymmdt"
        )

        # Create a collector 
        self.memory_limit = 0.8
        self.collector = mm.Collector(
            {'P01': self.dss},
            time_window_size=pd.Timedelta(seconds=2),
            memory_limit=self.memory_limit
        )

        # Create the logging queue and exiting event
        self.logging_queue = queue.Queue(maxsize=100)
        self.thread_exit = threading.Event()
        self.thread_exit.clear()

        # Adding the queue to the session
        self.session.set_logging_queue(self.logging_queue)
        self.session.set_thread_exit(self.thread_exit)

    def adding_test_data(self):
        
        # Timing
        start_time = pd.Timedelta(seconds=0)
        tab_end_time = pd.Timedelta(seconds=10)
        video_end_time = pd.Timedelta(seconds=0.5)
        
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

    def test_single_session_memory_management(self):

        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=30)
        self.collector.set_start_time(start_time)
        self.collector.set_end_time(end_time)
       
        # Need to add the queue externally
        loading_queue = queue.Queue(maxsize=1000)
        self.collector.set_loading_queue(loading_queue)
        
        # Starting the collector thread
        thread = self.collector.load_data_to_queue()
        thread.start()
        
        # Take snapshot of memory before
        pre_free = mm.tools.get_free_memory()

        # Clearing out the loading data queue
        while True:

            if self.collector.loading_queue.qsize() != 0:
                data = self.collector.loading_queue.get()

                # If end, stop it
                if data == 'END':
                    break
                else: # else, test logging

                    # Extract data
                    test_tabular_data = data['P01']['test_tabular']
                    test_video_data = data['P01']['test_video']

                    # Test all types of logging
                    self.session.add_tabular(
                        name='test_tabular',
                        data=test_tabular_data,
                        time_column='_time_'
                    )
                    # self.session.add_image(
                    #     name='test_image_with_timestamp',
                    #     data=test_video_data['frames'].iloc[0],
                    #     timestamp=test_video_data['_time_'].iloc[0]
                    # )
                    # self.session.add_images(
                    #     name='test_images',
                    #     df=test_video_data,
                    #     data_column='frames',
                    #     time_column='_time_'
                    # )
                    self.session.add_video(
                        name='test_video',
                        df=test_video_data,
                    )

                    # Log information
                    print(f"Used mem ratio from pre: {pre_free/mm.tools.get_free_memory()}")

        # Joining thread later
        thread.join()

        # Noting how much as used
        mid_free = pre_free - mm.tools.get_free_memory()

        print(f"Total used memory for logging: {mid_free}")

        # Then let the session save the log data
        # Create the thread
        thread = self.session.load_data_to_log(verbose=True)

        # Start the thread and tell it to quit
        thread.start()
        self.thread_exit.set()
        thread.join()

        # Take snapshot of memory and ensure the available memory reflects
        # the log data is removed.
        post_free = mm.tools.get_free_memory()

        # Calculate the diff of memory
        diff = np.abs(post_free - pre_free) / pre_free

        # This difference should be minimal
        assert diff < 0.1, f"Memory Leak of {diff}!"

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

        # assert expected_meta == actual_meta, \
        #     f"Generated meta file is not correct."

class MultipleSessionTestCase(unittest.TestCase):

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
        )

        # Create a list of the data streams
        dss = [self.tabular_ds, self.video_ds]

        # Clear out the previous pymmdt run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "pymmdt"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)
        
        # Create an overall session and pipeline
        self.total_session = mm.Session(
            log_dir = OUTPUT_DIR,
            experiment_name = "pymmdt"
        )

        # Now create multiple subsessions
        for x in range(1,3+1):
            self.total_session.create_subsession(f'P0{x}')

        # Creating containers for queues and the thread exit event
        self.logging_queues = []
        self.thread_exit = threading.Event()
        self.thread_exit.clear()
       
        # Initializing all sessions!
        for session in [self.total_session] + self.total_session.subsessions:
            # Create the logging queue and exiting event
            logging_queue = queue.Queue(maxsize=100)
            self.logging_queues.append(logging_queue)

            # Adding the queue to the session
            session.set_logging_queue(logging_queue)
            session.set_thread_exit(self.thread_exit)
    
    def adding_test_data(self, session):
        
        # Timing
        start_time = pd.Timedelta(seconds=0)
        tab_end_time = pd.Timedelta(seconds=10)
        video_end_time = pd.Timedelta(seconds=0.5)
        
        test_tabular_data = self.tabular_ds.get(start_time, tab_end_time)
        test_video_data = self.video_ds.get(start_time, video_end_time)

        # Test all types of logging
        session.add_tabular(
            name='test_tabular',
            data=test_tabular_data,
            time_column='_time_'
        )
        # session.add_image(
        #     name='test_image_without_timestamp',
        #     data=test_video_data['frames'].iloc[0]
        # )
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

    def test_subsession_logging(self):

        # For all the sessions
        for id, session in enumerate([self.total_session] + self.total_session.subsessions):
            # Add data
            self.adding_test_data(session)

            assert self.logging_queues[id].qsize() == 4
    
    def test_subsession_queuing_threading_saving_and_closing(self):
       
        # For all the sessions
        for id, session in enumerate([self.total_session] + self.total_session.subsessions):
            # Load the data into the queue
            self.adding_test_data(session)
            self.adding_test_data(session)

            # Create the thread
            thread = session.load_data_to_log()

            # Start the thread and tell it to quit
            thread.start()
            self.thread_exit.set()
            thread.join()

            # All the data should be processed
            assert self.logging_queues[id].qsize() == 0

        # Closing session to ensure that everything 
        self.total_session.close()
        
        for id, session in enumerate([self.total_session] + self.total_session.subsessions):

            # First, the files should exists now
            for entry in session.records.values():
                assert entry.save_loc.exists()

            # The estimated FPS should be close to the input FPS
            estimated_fps = session.records['test_video'].stream.fps
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
        with open(self.total_session.session_dir / "meta.json", "r") as json_file:
            actual_meta = json.load(json_file)
        
        # Convert all the str pd.Timedelta to actual
        for record_name in actual_meta['records'].keys():
            start_time = actual_meta['records'][record_name]['start_time']
            end_time = actual_meta['records'][record_name]['end_time']
            actual_meta['records'][record_name]['start_time'] = pd.to_timedelta(start_time)
            actual_meta['records'][record_name]['end_time'] = pd.to_timedelta(end_time)

        pprint.pprint(expected_meta)
        pprint.pprint(actual_meta)

        # assert expected_meta == actual_meta, \
        #     f"Generated meta file is not correct."

if __name__ == "__main__":
    unittest.main()

