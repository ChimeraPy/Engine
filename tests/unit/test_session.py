# Built-in Imports
import unittest
import threading
import pathlib
import shutil
import os
import queue

# Third-Party Imports
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
            name='test_image_without_timestamp',
            data=test_video_data['frames'].iloc[0]
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
        self.session.add_video_frames(
            name='test_video',
            df=test_video_data,
        )

    def test_single_session_logging_data(self):
        # Loading the data
        self.adding_test_data()
        
        # Check the number of log data matches the queue
        assert self.logging_queue.qsize() == 5

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
        session.add_image(
            name='test_image_without_timestamp',
            data=test_video_data['frames'].iloc[0]
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
        session.add_video_frames(
            name='test_video',
            df=test_video_data,
        )

    def test_subsession_logging(self):

        # For all the sessions
        for id, session in enumerate([self.total_session] + self.total_session.subsessions):
            # Add data
            self.adding_test_data(session)

            assert self.logging_queues[id].qsize() == 5
    
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

if __name__ == "__main__":
    unittest.main()

