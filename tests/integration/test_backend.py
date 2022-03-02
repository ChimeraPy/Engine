# Built-in Imports
from typing import Dict
import time
import unittest
import pathlib
import shutil
import os
import threading
import queue
import gc

# Third-Party Imports
import cv2
import pandas as pd
import tqdm
import numpy as np

# PyMMDT Library
import pymmdt as mm
import pymmdt.tabular as mmt
import pymmdt.video as mmv

# Testing package
# from . import test_doubles
# from .test_doubles import TestExamplePipe

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

# Creating test pipe class and instance
class TestExamplePipe(mm.Pipe):
    def step(self, data_samples: Dict[str, Dict[str, pd.DataFrame]]):
        self.session.add_tabular('test_tabular', data_samples['test_tabular'])
        self.session.add_video('test_video', data_samples['test_video'])
        self.session.add_images('test_images', data_samples['test_video'])

        # TODO -> This needs to work as well!
        # self.session.add_image('test_image', data_samples['test_video'].iloc[0].to_frame())

class NoRunnerButAllOtherComponents(unittest.TestCase):

    def setUp(self):
        
        # Storing the data
        csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
        csv_data['_time_'] = pd.to_timedelta(csv_data['time'], unit="s")

        # Create each type of data stream
        # self.tabular_ds = mmt.TabularDataStream(
        #     name="test_tabular",
        #     data=csv_data,
        #     time_column="_time_"
        # )
        # self.video_ds = mmv.VideoDataStream(
        #     name="test_video",
        #     start_time=pd.Timedelta(0),
        #     video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
        # )
        
        # # Create a list of the data streams
        # self.dss = [self.tabular_ds, self.video_ds]
        
        # # Clear out the previous pymmdt run 
        # # since pipeline is still underdevelopment
        # self.exp_dir = OUTPUT_DIR / "pymmdt"
        # if self.exp_dir.exists():
        #     shutil.rmtree(self.exp_dir)

        # # Construct the individual participant pipeline object
        # # Create an overall session and pipeline
        # self.session = mm.Session(
        #     log_dir = OUTPUT_DIR,
        #     experiment_name = "pymmdt"
        # )

        # # Create a collector 
        # self.memory_limit = 0.8
        # self.collector = mm.Collector(
        #     {'P01': self.dss},
        #     time_window_size=pd.Timedelta(seconds=2),
        #     memory_limit=self.memory_limit
        # )

        # # Create the logging queue and exiting event
        # self.logging_queue = queue.Queue(maxsize=100)
        # self.thread_exit = threading.Event()
        # self.thread_exit.clear()

        # # Adding the queue to the session
        # self.session.set_logging_queue(self.logging_queue)
        # self.session.set_thread_exit(self.thread_exit)

    def test_run_with_loading_and_logging_threads(self):

        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=30)
        self.collector.set_start_time(start_time)
        self.collector.set_end_time(end_time)
       
        # Need to add the queue externally
        loading_queue = queue.Queue(maxsize=1000)
        self.collector.set_loading_queue(loading_queue)

        # Creating the pipe
        pipe = TestExamplePipe()
        pipe.attach_session(self.session)
        
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

                    # Get the data samples for pipeline 
                    data_sample = data['P01']

                    # Passing the data throught the pipe
                    pipe.step(data_sample)

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
    
    def test_all_threads_run_separately(self):

        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=30)
        self.collector.set_start_time(start_time)
        self.collector.set_end_time(end_time)
       
        # Need to add the queue externally
        loading_queue = queue.Queue(maxsize=1000)
        self.collector.set_loading_queue(loading_queue)

        # Creating the pipe
        pipe = TestExamplePipe()
        pipe.attach_session(self.session)
        
        def process_data():

            # Continue iterating
            while True:

                # Retrieveing sample from the loading queue
                all_data_samples = loading_queue.get(block=True)
                loading_queue.task_done()
               
                # Check for end condition
                if all_data_samples == 'END':
                    break

                # Then propagate the sample throughout the pipe
                pipe.step(all_data_samples['P01'])

                # Deleting the memory
                del all_data_samples
                gc.collect()
                
        # Starting the collector thread
        loading_thread = self.collector.load_data_to_queue()
        loading_thread.start()
        
        # Take snapshot of memory before
        pre_free = mm.tools.get_free_memory()

        # Joining thread later
        loading_thread.join()
       
        # Noting how much as used
        mid_free = pre_free - mm.tools.get_free_memory()
        print(f"Total used memory for loading: {mid_free}")

        # Start processing thread
        processing_thread = threading.Thread(target=process_data)
        processing_thread.start()
        processing_thread.join()

        # Noting how much after processing
        mid_free = pre_free - mm.tools.get_free_memory()
        print(f"Total used memory for processing: {mid_free}")

        # Then let the session save the log data
        # Create the thread
        logging_thread = self.session.load_data_to_log(verbose=True)

        # Start the thread and tell it to quit
        logging_thread.start()
        self.thread_exit.set()
        logging_thread.join()

        # Take snapshot of memory and ensure the available memory reflects
        # the log data is removed.
        post_free = mm.tools.get_free_memory()

        # Calculate the diff of memory
        diff = np.abs(post_free - pre_free) / pre_free

        # This difference should be minimal
        assert diff < 0.1, f"Memory Leak of {diff}!"
    
    def test_all_threads_run_simulatenous(self):

        start_time = pd.Timedelta(seconds=0)
        end_time = pd.Timedelta(seconds=30)
        # self.collector.set_start_time(start_time)
        # self.collector.set_end_time(end_time)
       
        # Need to add the queue externally
        loading_queue = queue.Queue(maxsize=1000)
        # self.collector.set_loading_queue(loading_queue)

        # Creating the pipe
        # pipe = TestExamplePipe()
        # pipe.attach_session(self.session)
        
        # Take snapshot of memory and ensure the available memory reflects
        # the log data is removed.
        pre_free = mm.tools.get_free_memory()

        def load_data():

            # Load the video
            input_video = RAW_DATA_DIR / 'example_use_case' / 'test_video1.mp4'
            video = cv2.VideoCapture(str(input_video))
            
            start_time = pd.Timedelta(seconds=0)
            end_time = pd.Timedelta(seconds=3)
            time_window = pd.Timedelta(seconds=3)
            
            for i in range(5):

                # Get data and put on the queue
                # TODO: FIX THIS ISSUE
                # data = self.video_ds.get(start_time, end_time)
                # frames = []
                # for i in range(60*3):
                #     res, frame = video.read()
                #     frames.append(frame)
                # data = pd.DataFrame({'frames': frames})

                data_chunk = {
                    'P01': {
                        'test_video': data
                    }
                }
                loading_queue.put(data_chunk)

                # Update
                start_time += time_window
                end_time += time_window

            # Once over, send end message
            loading_queue.put("END")
        
        def process_data():

            # Creating place to write video
            OUTPUT_VIDEO = OUTPUT_DIR / 'test_video.avi'
            FPS = 30
            H, W = 720, 1280
            writer = cv2.VideoWriter()
            writer.open(
                str(OUTPUT_VIDEO),
                cv2.VideoWriter_fourcc(*'DIVX'),
                FPS,
                (W, H),
            )

            # Continue iterating
            while True:

                # Retrieveing sample from the loading queue
                all_data_samples = loading_queue.get(block=True)
                loading_queue.task_done()
               
                # Check for end condition
                if all_data_samples == 'END':
                    break

                # Then propagate the sample throughout the pipe
                # pipe.step(all_data_samples['P01'])
                data_samples = all_data_samples['P01']
                video = data_samples['test_video']
                for index, row in video.iterrows():
                    frame = getattr(row, 'frames')
                    writer.write(np.uint8(frame).copy())

                # Deleting the memory
                del all_data_samples
                gc.collect()

            # Then release the video
            writer.release()
                
        # Starting the collector thread
        # loading_thread = self.collector.load_data_to_queue()
        loading_thread = threading.Thread(target=load_data)
        loading_thread.start()
        
        # Start processing thread
        processing_thread = threading.Thread(target=process_data)
        processing_thread.start()

        # Then let the session save the log data
        # Create the thread
        # logging_thread = self.session.load_data_to_log(verbose=True)
        # logging_thread.start()
      
        # Stopping all threads
        loading_thread.join()
        print("Loading done!")
        processing_thread.join()
        print("Processing done!")
        # self.thread_exit.set()
        # logging_thread.join()
        # print("Logging done!")

        # Take snapshot of memory and ensure the available memory reflects
        # the log data is removed.
        post_free = mm.tools.get_free_memory()

        # Calculate the diff of memory
        diff = np.abs(post_free - pre_free) / pre_free

        # This difference should be minimal
        assert diff < 0.1, f"Memory Leak of {diff}!"

class SingleRunnerBackEndTestCase(unittest.TestCase):
    
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
            fps=30
        )
        
        # Clear out the previous pymmdt run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "pymmdt"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)

        # Construct the individual participant pipeline object
        # Create an overall session and pipeline
        self.session = mm.Session(
            log_dir = OUTPUT_DIR,
            experiment_name = "pymmdt"
        )

        # Use a test pipeline
        # individual_pipeline = test_doubles.TestExamplePipe()
        individual_pipeline = TestExamplePipe()
        
        # end_time = pd.Timedelta(seconds=30)

        # Load construct the first runner
        self.runner = mm.SingleRunner(
            name='P01',
            data_streams=[self.tabular_ds, self.video_ds],
            pipe=individual_pipeline,
            session=self.session,
            time_window_size=pd.Timedelta(seconds=2),
            # end_at=end_time,
            run_solo=True,
            memory_limit=0.05
        )

    def test_single_runner_run(self):
        
        # Take snapshot of memory before
        pre_free = mm.tools.get_free_memory()
        
        # Run the runner with everything set
        # self.runner.run(verbose=True)
        self.runner.run()
        
        # Take snapshot of memory and ensure the available memory reflects
        # the log data is removed.
        post_free = mm.tools.get_free_memory()

        # Calculate the diff of memory
        diff = np.abs(post_free - pre_free) / pre_free

        # This difference should be minimal
        assert diff < 0.1, f"Memory Leak of {diff}!"
        
        # The estimated FPS should be close to the input FPS
        estimated_fps = self.session.records['test_video'].stream.fps
        actual_fps = self.video_ds.fps
        assert estimated_fps == actual_fps, \
            f"Estimate FPS: {estimated_fps} vs. Actual FPS: {actual_fps}"

        # Then number of frames between the video should also match
        expected_frames = len(self.video_ds)
        actual_frames = len(self.session.records['test_video'].stream)
        assert expected_frames == actual_frames, \
            f"Expected num of frames: {expected_frames} vs. Actual {actual_frames}"

class GroupRunnerBackEndTestCase(unittest.TestCase):
    
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
            fps=30
        )
        
        # Clear out the previous pymmdt run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "pymmdt"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)
        
        # Create a list of the data streams
        dss = [self.tabular_ds, self.video_ds]

        # Then for each participant, we need to setup their own session,
        # pipeline, and runner
        self.runners = []
        for x in range(1,2+1):
            
            # Use a test pipeline
            # individual_pipeline = test_doubles.TestExamplePipe()
            individual_pipeline = TestExamplePipe()

            runner = mm.SingleRunner(
                name=f"P0{x}",
                data_streams=dss.copy(),
                pipe=individual_pipeline,
            )

            # Store the individual's runner to a list 
            self.runners.append(runner)

        # Construct the individual participant pipeline object
        # Create an overall session and pipeline
        self.total_session = mm.Session(
            log_dir = OUTPUT_DIR,
            experiment_name = "pymmdt"
        )

        # Load construct the first runner
        self.runner = mm.GroupRunner(
            name="Teamwork Example #1",
            pipe=mm.Pipe(),
            runners=self.runners, 
            session=self.total_session,
            time_window_size=pd.Timedelta(seconds=3),
        )

    def test_group_runner_run(self):
        
        # Run the runner with everything set
        self.runner.run(verbose=True)
        # self.runner.run()
        
        for id, session in enumerate([self.total_session] + self.total_session.subsessions):
            # The estimated FPS should be close to the input FPS
            if 'test_video' in session.records.keys():
                estimated_fps = session.records['test_video'].stream.fps
                actual_fps = self.video_ds.fps
                assert estimated_fps == actual_fps, \
                    f"Estimate FPS: {estimated_fps} vs. Actual FPS: {actual_fps}"

                # Then number of frames between the video should also match
                expected_frames = len(self.video_ds)
                actual_frames = len(session.records['test_video'].stream)
                assert expected_frames == actual_frames, \
                    f"Expected num of frames: {expected_frames} vs. Actual {actual_frames}"
        
if __name__ == "__main__":
    # unittest.main()
    test = GroupRunnerBackEndTestCase()
    test.setUp()
    test.test_group_runner_run()
