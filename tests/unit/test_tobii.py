# Built-in Imports
from typing import Dict
import unittest
import pathlib
import shutil
import os
import sys
import time
import collections
import queue

# Third-Party Imports
import tqdm
import pandas as pd

# Testing Library
import pymmdt as mm
import pymmdt.utils.tobii

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = ROOT_DIR / 'data' 
OUTPUT_DIR = ROOT_DIR / 'test_output' 

sys.path.append(str(ROOT_DIR))

class TestTobiiExamplePipe(mm.Pipe):
    def step(self, data_samples: Dict[str, Dict[str, pd.DataFrame]]):
        data_streams_samples = list(data_samples.values())[0]
        self.session.add_video('test_video', data_streams_samples['video'])

class TobiiTestCase(unittest.TestCase):

    def test_load_tobii_one_participant(self):

        # Load the data for all participants (ps)
        one_participant_dir = RAW_DATA_DIR / 'nurse_use_case' / '20211029T140731Z'
        ps_dss = pymmdt.utils.tobii.load_participant_data(one_participant_dir, verbose=True)

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
        individual_pipeline = TestTobiiExamplePipe()

        # Load construct the first runner
        self.runner = mm.SingleRunner(
            name='P01',
            data_streams=ps_dss,
            pipe=individual_pipeline,
            session=self.session,
            time_window_size=pd.Timedelta(seconds=1),
            end_at=pd.Timedelta(seconds=10),
            run_solo=True,
        )

        # Run the runner
        self.runner.run(verbose=True)
        # self.runner.run()

    def test_load_tobii_session(self):

        # Load the data for all participants (ps)
        session_dir = RAW_DATA_DIR / 'nurse_use_case'
        pss_dss = pymmdt.utils.tobii.load_session_data(session_dir, verbose=True)

        # Clear out the previous pymmdt run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "pymmdt"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)

        # Construct the individual participant pipeline object
        # Create an overall session and pipeline
        self.total_session = mm.Session(
            log_dir = OUTPUT_DIR,
            experiment_name = "pymmdt"
        )

        # Create all the participant runners
        self.runners = []
        for ps_id, ps_data in pss_dss.items():
            
            # Use a test pipeline
            # individual_pipeline = test_doubles.TestExamplePipe()
            individual_pipeline = TestTobiiExamplePipe()

            # Extracting the dss (data streams)
            dss = ps_data['data']

            runner = mm.SingleRunner(
                name=f"{ps_id}",
                data_streams=dss.copy(),
                pipe=individual_pipeline,
            )

            # Store the individual's runner to a list 
            self.runners.append(runner)

        # Load construct the first runner
        self.runner = mm.GroupRunner(
            name="Teamwork Example #1",
            pipe=mm.Pipe(),
            runners=self.runners, 
            session=self.total_session,
            end_at=pd.Timedelta(seconds=10),
            time_window_size=pd.Timedelta(seconds=1),
        )

        # Run the runner
        self.runner.run(verbose=True)
