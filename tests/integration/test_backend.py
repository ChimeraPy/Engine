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

# This class contains all the test we want to conduct
# All the children class have different data for testing different 
# scenarios

class BackEndTestCase(object):
    
    def test_single_runner_and_collector_together(self):

        # Start 
        self.runner.start()

        # And start the threads
        self.runner.loading_thread.start()
        self.runner.processing_thread.start()

        # Creating a loading bar to show the step of processing data
        pbar = tqdm.tqdm(total=len(self.runner.collector.windows), desc="Processing data")
        last_value = 0

        # Update the loading bar is it continues
        while True:
            if last_value != self.runner.num_processed_data_chunks:
                diff = self.runner.num_processed_data_chunks - last_value
                last_value = self.runner.num_processed_data_chunks
                pbar.update(diff)

            if last_value == len(self.runner.collector.windows):
                break

        # And wait until the threads stop
        self.runner.processing_thread.join()
        self.runner.loading_thread.join()

    def test_single_runner_and_session_together(self):
        
        # Start 
        self.runner.start()
        
        # And start the threads
        self.runner.processing_thread.start()
        for thread in self.runner.logging_threads:
            thread.start()

        sample_data = {
            self.runner.name: {
                'test': pd.DataFrame({'a':[1], 'b': [1]})
            }
        }
        end_sample_data = {
            'END'
        }

        self.runner.logging_queues[0].put(sample_data)
        self.runner.logging_queues[0].put(end_sample_data)
        
        # And wait until the threads stop
        self.runner.processing_thread.join()
        for thread in self.runner.logging_threads:
            thread.join()

        # Check the session indeed save the data
        entry = self.session.records['test']
        assert entry.file.exists()

    def test_single_runner_collector_and_session_together(self):
        ...

    def test_single_runner_run(self):
        ...

class ExampleDataTestCase(BackEndTestCase, unittest.TestCase):
    
    def setUp(self):

        # Storing the data
        csv_data = pd.read_csv(RAW_DATA_DIR/"example_use_case"/"test.csv")
        csv_data['_time_'] = pd.to_timedelta(self.csv_data['time'], unit="s")

        # Create each type of data stream
        self.tabular_ds = mmt.TabularDataStream(
            name="test_tabular",
            data=self.csv_data,
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
        individual_pipeline = test_doubles.TestPipe()

        # Load construct the first runner
        self.runner = mm.SingleRunner(
            name='P01',
            data_streams=[self.tabular_ds, self.video_ds],
            pipe=individual_pipeline,
            session=self.session,
            time_window_size=pd.Timedelta(seconds=3),
            run_solo=True,
            verbose=True
        )

if __name__ == "__main__":
    unittest.main()
