# Built-in Imports
import unittest
import pathlib
import shutil
import os

# Testing Library
import pymmdt as mm
import pymmdt.utils as mmu

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
RAW_DATA_DIR = CURRENT_DIR / 'data' 
OUTPUT_DIR = CURRENT_DIR / 'test_output' 

class NurseTestCase(unittest.TestCase):

    def test_integration(self):

        # Load the data for all participants (ps)
        nursing_session_dir = RAW_DATA_DIR / 'nurse_use_case'
        ps = mmu.tobii.load_session_data(nursing_session_dir)

        # Clear out the previous pymmdt run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "pymmdt"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)

        # Create an overall session and pipeline
        total_session = mm.Session(
            log_dir = OUTPUT_DIR,
            experiment_name = "pymmdt"
        )
        data_pipeline = mm.Pipe()

        # Then for each participant, we need to setup their own session,
        # pipeline, and runner
        runners = []
        for p_id, p_elements in ps.items():

            # Create user pipe and session
            participant_pipe = data_pipeline.copy()
            participant_session = total_session.create_subsession(
                name=p_id
            )

            # Create the Runner
            runner = mm.Runner(
                name=p_id,
                data_streams=p_elements['data'],
                pipe=participant_pipe,
                session=participant_session,
                time_window_size=pd.Timdelta(seconds=10)
            )

            # Store the individual's runner to a list 
            runners.append(runner)

        # Pass all the runners to the Director
        director = mm.Director(runners, total_session)

        # Then execute run for the entire session director
        director.run()

if __name__ == "__main__":
    # Run when debugging is not needed
    # unittest.main()

    # Otherwise, we have to call the test ourselves
    test_case = NurseTestCase()
    test_case.test_integration()
