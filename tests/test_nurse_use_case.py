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

        # Then for each participant, we need to setup their own session,
        # pipeline, and runner
        workers = []
        for p_id, p_elements in ps.items():
            
            # Construct the individual participant pipeline object
            individual_pipeline = mm.Pipe()

            worker = mm.SingleWorker(
                name=p_id,
                data_streams=p_elements['data'],
                pipe=individual_pipeline,
            )

            # Store the individual's runner to a list 
            workers.append(worker)
        
        # Create an overall session and pipeline
        total_session = mm.Session(
            log_dir = OUTPUT_DIR,
            experiment_name = "pymmdt"
        )
        overall_pipeline = mm.Pipe()

        # Pass all the runners to the Director
        director = mm.GroupWorker(
            name="Nurse Teamwork Example #1",
            pipe=overall_pipeline,
            workers=workers, 
            session=total_session,
            time_window_size=pd.Timedelta(seconds=5)
        )

        # Then execute run for the entire session director
        director.run()

if __name__ == "__main__":
    # Run when debugging is not needed
    # unittest.main()

    # Otherwise, we have to call the test ourselves
    test_case = NurseTestCase()
    test_case.test_integration()
