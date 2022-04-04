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

# ChimeraPy Library
import chimerapy as cp

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

#  reating test pipe class and instance
class TestExamplePipeline(cp.Pipeline):
    def step(self, data_samples: Dict[str, Dict[str, pd.DataFrame]]):
        self.session.add_tabular('test_tabular', data_samples['test_tabular'])
        self.session.add_video('test_video', data_samples['test_video'])
        self.session.add_images('test_images', data_samples['test_video'])

class SingleRunnerBackEndTestCase(unittest.TestCase):
    
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
            fps=30
        )

        # Clearing out the previous session
        experiment_name = 'P01'
        experiment_dir = OUTPUT_DIR / experiment_name
        if experiment_dir.exists():
            shutil.rmtree(experiment_dir)
        
        # Use a test pipeline
        # individual_pipeline = test_doubles.TestExamplePipeline()
        self.individual_pipeline = TestExamplePipeline()
        
    def test_single_short_runner_run(self):
        
        end_time = pd.Timedelta(seconds=30)

        # Load construct the first runner
        self.runner = cp.SingleRunner(
            logdir=OUTPUT_DIR,
            name='P01',
            data_streams=[self.tabular_ds, self.video_ds],
            pipe=self.individual_pipeline,
            time_window=pd.Timedelta(seconds=1),
            end_time=end_time,
            run_solo=True,
            memory_limit=0.8
        )
        
        # Run the runner with everything set
        self.runner.run(verbose=True)
        # self.runner.run()
        
    def test_single_memory_stress_runner_run(self):
        
        # Load construct the first runner
        self.runner = cp.SingleRunner(
            logdir=OUTPUT_DIR,
            name='P01',
            data_streams=[self.tabular_ds, self.video_ds],
            pipe=self.individual_pipeline,
            time_window=pd.Timedelta(seconds=0.5),
            run_solo=True,
            memory_limit=0.5
        )
        
        # Run the runner with everything set
        self.runner.run(verbose=True)
        # self.runner.run()
        
class GroupRunnerBackEndTestCase(unittest.TestCase):
    
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
            fps=30
        )
        
        # Clear out the previous ChimeraPy run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "ChimeraPy"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)
        
        # Create a list of the data streams
        dss = [self.tabular_ds, self.video_ds]

        # Then for each participant, we need to setup their own session,
        # pipeline, and runner
        self.runners = []
        for x in range(2):
            
            # Use a test pipeline
            individual_pipeline = TestExamplePipeline()

            runner = cp.SingleRunner(
                name=f"P0{x}",
                data_streams=dss.copy(),
                pipe=individual_pipeline,
            )

            # Store the individual's runner to a list 
            self.runners.append(runner)

        # Load construct the first runner
        self.runner = cp.GroupRunner(
            logdir=OUTPUT_DIR,
            name="ChimeraPy",
            pipe=cp.Pipeline(),
            runners=self.runners, 
            # end_time=pd.Timedelta(seconds=5),
            time_window=pd.Timedelta(seconds=0.5),
            memory_limit=0.5
        )

    def test_group_runner_run(self):
        
        # Run the runner with everything set
        self.runner.run(verbose=True)
        # self.runner.run()
        
if __name__ == "__main__":
    unittest.main()
    # test = SingleRunnerBackEndTestCase()
    # test.setUp()
    # test.test_single_short_runner_run()
