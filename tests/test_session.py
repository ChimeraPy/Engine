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

# Testing Library
import pymmdt as mm
import pymmdt.utils.tobii
import test_doubles

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = CURRENT_DIR / 'data' 
OUTPUT_DIR = CURRENT_DIR / 'test_output' 

sys.path.append(str(ROOT_DIR))

class SingleRunnerTestCase(unittest.TestCase):

    def setUp(self):

        # Load the data for all participants (ps)
        nursing_session_dir = RAW_DATA_DIR / 'nurse_use_case'
        ps = pymmdt.utils.tobii.load_session_data(nursing_session_dir, verbose=True)
        
        # Clear out the previous pymmdt run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "pymmdt"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)

        # Participant information
        p_ids, p_elements = list(ps.keys()), list(ps.values())
            
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
            name=p_ids[0],
            data_streams=p_elements[0]['data'],
            pipe=individual_pipeline,
            session=self.session,
            time_window_size=pd.Timedelta(seconds=10),
            run_solo=True,
            verbose=True
        )

    def test_logging_queue_for_single_runner_with_tabular_data(self):

        # Start 
        self.runner.start()
       
        # Starting the session's threads
        for thread in self.runner.logging_threads:
            thread.start()

        # Creating sample data
        sample_data = {
            self.runner.name: {
                'test': pd.DataFrame({'a':[1], 'b': [1]})
            }
        }
        sample2_data = {
            self.runner.name: {
                'test': pd.DataFrame({'a':[2], 'b': [2]})
            }
        }
        end_sample_data = {
            'END'
        }

        self.runner.logging_queues[0].put(sample_data)
        self.runner.logging_queues[0].put(sample2_data)
        self.runner.logging_queues[0].put(end_sample_data)

        # Wait until the whole data is processed 
        for thread in self.runner.logging_threads:
            thread.join()

        # Close the files and finish writing them
        self.session.close()

        # Now check the file generated and ensure that the contents are
        # correct
        saved_data = pd.read_csv(self.session.file)
        expected_data = pd.concat(sample_data, sample2_data)

        assert saved_data == expected_data

if __name__ == "__main__":
    unittest.main()

