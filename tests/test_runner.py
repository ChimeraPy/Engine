# Built-in Imports
import time
import unittest
import pathlib
import shutil
import os
import sys

# Third-Party Imports
import pandas as pd

# Testing Library
import pymmdt as mm
import pymmdt.utils.tobii

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = CURRENT_DIR / 'data' 
OUTPUT_DIR = CURRENT_DIR / 'test_output' 

sys.path.append(str(ROOT_DIR))

class GroupRunnerTestCase(unittest.TestCase):

    def setUp(self):

        # Load the data for all participants (ps)
        nursing_session_dir = RAW_DATA_DIR / 'nurse_use_case'
        ps = pymmdt.utils.tobii.load_session_data(nursing_session_dir, verbose=True)

        # Clear out the previous pymmdt run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "pymmdt"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)

        # Then for each participant, we need to setup their own session,
        # pipeline, and runner
        self.workers = []
        for p_id, p_elements in ps.items():
            
            # Construct the individual participant pipeline object
            individual_pipeline = mm.Pipe()

            worker = mm.SingleRunner(
                name=p_id,
                data_streams=p_elements['data'],
                pipe=individual_pipeline,
            )

            # Store the individual's runner to a list 
            self.workers.append(worker)
        
        # Create an overall session and pipeline
        total_session = mm.Session(
            log_dir = OUTPUT_DIR,
            experiment_name = "pymmdt"
        )
        overall_pipeline = mm.Pipe()

        # Pass all the runners to the Director
        self.director = mm.GroupRunner(
            name="Nurse Teamwork Example #1",
            pipe=overall_pipeline,
            workers=self.workers, 
            session=total_session,
            time_window_size=pd.Timedelta(seconds=5)
        )

    def test_runner_group_run(self):

        # Run the director
        self.director.run()

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
        total_session = mm.Session(
            log_dir = OUTPUT_DIR,
            experiment_name = "pymmdt"
        )
        individual_pipeline = mm.Pipe()

        # Load construct the first runner
        self.runner = mm.SingleRunner(
            name=p_ids[0],
            data_streams=p_elements[0]['data'],
            pipe=individual_pipeline,
            session=total_session,
            time_window_size=pd.Timedelta(seconds=10),
            run_solo=True,
            verbose=True
        )

    def test_runner_get_data_and_step_process(self):

        # Make the collector get and store samples in the queue
        print("Letting data collected for 1 second")
        time.sleep(1)

        print("Closing collector")
        self.runner.collector.close()

        # Now extract the samples and step process them
        print("Iterating over all the accumulated samples")
        while self.runner.collector.loading_queue.qsize() != 0:

            # Forward propagate through pipe
            print(".", end="")
            self.runner.get_and_step()

        print("")

    def test_runner_run(self):

        # Run the runner
        self.runner.run()

if __name__ == "__main__":
    # Run when debugging is not needed
    # unittest.main()

    # Otherwise, we have to call the test ourselves
    # test_case = GroupNurseTestCase()
    single_test_case = SingleRunnerTestCase()
    single_test_case.setUp()
    single_test_case.test_runner_get_data_and_step_process()
