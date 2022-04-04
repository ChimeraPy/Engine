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
import chimerapy as cp
import chimerapy.utils.tobii

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = ROOT_DIR / 'data' 
OUTPUT_DIR = ROOT_DIR / 'test_output' 

sys.path.append(str(ROOT_DIR))

class TestTobiiExamplePipeline(cp.Pipeline):
    def step(self, data_samples: Dict[str, Dict[str, pd.DataFrame]]):
        self.session.add_video('test_video', data_samples['video'])

class TobiiTestCase(unittest.TestCase):

    def test_load_tobii_one_participant(self):

        # Load the data for all participants (ps)
        one_participant_dir = RAW_DATA_DIR / 'nurse_use_case' / '20211029T140731Z'
        participant_session = chimerapy.utils.tobii.load_single_session(one_participant_dir, verbose=True)

        # Clear out the previous ChimeraPy run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "P01"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)

        # Use a test pipeline
        # individual_pipeline = test_doubles.TestExamplePipeline()
        individual_pipeline = TestTobiiExamplePipeline()

        # Load construct the first runner
        self.runner = cp.SingleRunner(
            logdir=OUTPUT_DIR,
            name='P01',
            data_streams=participant_session['data'],
            pipe=individual_pipeline,
            time_window=pd.Timedelta(seconds=1),
            end_time=pd.Timedelta(seconds=10),
            run_solo=True,
        )

        # Run the runner
        self.runner.run(verbose=True)
        # self.runner.run()

    def test_load_tobii_session(self):

        # Load the data for all participants (ps)
        session_dir = RAW_DATA_DIR / 'nurse_use_case'
        participant_sessions = chimerapy.utils.tobii.load_multiple_sessions_in_one_directory(session_dir, verbose=True)

        # Clear out the previous ChimeraPy run 
        # since pipeline is still underdevelopment
        exp_dir = OUTPUT_DIR / "ChimeraPy"
        if exp_dir.exists():
            shutil.rmtree(exp_dir)

        # Create all the participant runners
        self.runners = []
        for ps_id, ps_data in participant_sessions.items():
            
            # Use a test pipeline
            individual_pipeline = TestTobiiExamplePipeline()

            # Extracting the dss (data streams)
            dss = ps_data['data']

            runner = cp.SingleRunner(
                name=f"{ps_id}",
                data_streams=dss.copy(),
                pipe=individual_pipeline,
            )

            # Store the individual's runner to a list 
            self.runners.append(runner)

        # Construct the group runner
        self.runner = cp.GroupRunner(
            logdir=OUTPUT_DIR,
            name="Teamwork Example #1",
            pipe=cp.Pipeline(),
            runners=self.runners, 
            end_time=pd.Timedelta(seconds=10),
            time_window=pd.Timedelta(seconds=1),
        )

        # Run the group runner
        # self.runner.run()
        self.runner.run(verbose=True)
