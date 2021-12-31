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
        
if __name__ == "__main__":
    unittest.main()
