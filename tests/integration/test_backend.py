# Built-in Imports
import time
import unittest
import pathlib
import shutil
import os
import sys

# Third-Party Imports
import pandas as pd
import tqdm

# PyMMDT Library
import pymmdt as mm
import pymmdt.tabular as mmt
import pymmdt.video as mmv

# Testing package
from . import test_doubles

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

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
        individual_pipeline = test_doubles.TestExamplePipe()

        # Load construct the first runner
        self.runner = mm.SingleRunner(
            name='P01',
            data_streams=[self.tabular_ds, self.video_ds],
            pipe=individual_pipeline,
            session=self.session,
            time_window_size=pd.Timedelta(seconds=3),
            run_solo=True,
        )

    def test_single_runner_run(self):
        
        # Run the runner with everything set
        self.runner.run(verbose=True)
        # self.runner.run()
        
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
            individual_pipeline = test_doubles.TestExamplePipe()

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
            name="Nurse Teamwork Example #1",
            pipe=mm.Pipe(),
            runners=self.runners, 
            session=self.total_session,
            time_window_size=pd.Timedelta(seconds=3),
        )

    def test_group_runner_run(self):
        
        # Run the runner with everything set
        self.runner.run(verbose=True)
        
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
    unittest.main()
