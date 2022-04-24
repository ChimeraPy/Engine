# Built-in Imports
import time
import signal
import unittest
import pathlib
import shutil
import os
import sys
import threading

# Third-Party Imports
import pandas as pd
import tqdm

# ChimeraPy Library
import chimerapy as cp

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

class SingleRunnerTestCase(unittest.TestCase):

    def setUp(self):

        # Storing the data
        csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
        csv_data['_time_'] = pd.to_timedelta(csv_data['time'], unit="s")

        # Create each type of data stream
        tabular_ds = cp.TabularDataStream(
            name="test_tabular",
            data=csv_data,
            time_column="_time_"
        )
        video_ds = cp.VideoDataStream(
            name="test_video",
            start_time=pd.Timedelta(0),
            video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
        )
        
        # Create a list of the data streams
        self.dss = [tabular_ds, video_ds]
        
        # Clear out the previous ChimeraPy run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "ChimeraPy"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)

        # Use a test pipeline
        # self.individual_pipeline = test_doubles.TestPipeline()
        self.individual_pipeline = cp.Pipeline()

    def test_runner_to_run(self):
        
        # Load construct the first runner
        self.runner = cp.SingleRunner(
            name='P01',
            logdir=OUTPUT_DIR,
            data_streams=self.dss,
            pipe=self.individual_pipeline,
            time_window=pd.Timedelta(seconds=0.5),
            run_solo=True,
        )

        # Running should be working!
        self.runner.run()

        return None

    def test_runner_run_with_shorter_sections(self):

        # Load construct the first runner
        self.runner = cp.SingleRunner(
            name='P01',
            logdir=OUTPUT_DIR,
            data_streams=self.dss,
            pipe=self.individual_pipeline,
            time_window=pd.Timedelta(seconds=0.5),
            start_time=pd.Timedelta(seconds=5),
            end_time=pd.Timedelta(seconds=10),
            run_solo=True,
        )

        # Running should be working!
        self.runner.run()

        return None

    def test_runner_handling_keyboard_interrupt(self):

        def create_keyboard_interrupt():
            time.sleep(1)
            signal.raise_signal(signal.SIGINT)
            print("KEYBOARD INTERRUPT!")
        
        # Load construct the first runner
        self.runner = cp.SingleRunner(
            name='P01',
            logdir=OUTPUT_DIR,
            data_streams=self.dss,
            pipe=self.individual_pipeline,
            time_window=pd.Timedelta(seconds=0.5),
            start_time=pd.Timedelta(seconds=0),
            end_time=pd.Timedelta(seconds=20),
            run_solo=True,
            verbose=True
        )

        # Create thread that later calls the keyboard interrupt signal
        signal_thread = threading.Thread(target=create_keyboard_interrupt, args=())
        signal_thread.start()

        # Run!
        self.runner.run()

        # Then stoping the thread
        signal_thread.join()
    
class GroupRunnerTestCase(unittest.TestCase):

    def setUp(self):
        
        # Storing the data
        csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
        csv_data['_time_'] = pd.to_timedelta(csv_data['time'], unit="s")

        # Create each type of data stream
        tabular_ds = cp.TabularDataStream(
            name="test_tabular",
            data=csv_data,
            time_column="_time_"
        )
        video_ds = cp.VideoDataStream(
            name="test_video",
            start_time=pd.Timedelta(0),
            video_path=RAW_DATA_DIR/"example_use_case"/"test_video1.mp4",
        )

        # Create a list of the data streams
        dss = [tabular_ds, video_ds]

        # Clear out the previous ChimeraPy run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "ChimeraPy"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)

        # Then for each participant, we need to setup their own session,
        # pipeline, and runner
        self.runners = []
        for x in range(2):
            
            # Construct the individual participant pipeline object
            individual_pipeline = cp.Pipeline()

            runner = cp.SingleRunner(
                name=f"P0{x}",
                data_streams=dss.copy(),
                pipe=individual_pipeline,
            )

            # Store the individual's runner to a list 
            self.runners.append(runner)
        
        # Create an overall session and pipeline
        self.overall_pipeline = cp.Pipeline()

    def test_group_runner_run(self):
        
        # Pass all the runners to the Director
        group_runner = cp.GroupRunner(
            logdir=OUTPUT_DIR,
            name="Nurse Teamwork Example #1",
            pipe=self.overall_pipeline,
            runners=self.runners, 
            time_window=pd.Timedelta(seconds=0.5),
        )

        # Run the director
        group_runner.run()

        return None

    def test_group_runner_with_shorter_run(self):
        
        # Pass all the runners to the Director
        group_runner = cp.GroupRunner(
            logdir=OUTPUT_DIR,
            name="Nurse Teamwork Example #1",
            pipe=self.overall_pipeline,
            runners=self.runners, 
            time_window=pd.Timedelta(seconds=0.5),
            start_time=pd.Timedelta(seconds=5),
            end_time=pd.Timedelta(seconds=15),
        )

        # Run the director
        group_runner.run()

        return None

    def test_group_runner_run_with_keyboard_interrupt_no_tui(self):
        
        def create_keyboard_interrupt():
            time.sleep(1)
            signal.raise_signal(signal.SIGINT)
        
        # Pass all the runners to the Director
        group_runner = cp.GroupRunner(
            logdir=OUTPUT_DIR,
            name="Nurse Teamwork Example #1",
            pipe=self.overall_pipeline,
            runners=self.runners, 
            time_window=pd.Timedelta(seconds=0.5),
            verbose=True
        )
        
        # Create thread that later calls the keyboard interrupt signal
        signal_thread = threading.Thread(target=create_keyboard_interrupt, args=())
        signal_thread.start()

        # Run the director
        group_runner.run()
        
        # Then stoping the thread
        signal_thread.join()

        return None
    
if __name__ == "__main__":
    # Run when debugging is not needed
    unittest.main()

    # Otherwise, we have to call the test ourselves
    # test_case = SingleRunnerTestCase()
    # test_case.setUp()
    # test_case.test_runner_to_run()
