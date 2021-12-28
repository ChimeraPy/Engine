# Built-in Imports
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
RAW_DATA_DIR = CURRENT_DIR / 'data' 
OUTPUT_DIR = CURRENT_DIR / 'test_output' 

sys.path.append(str(ROOT_DIR))

class TobiiTestCase(unittest.TestCase):

    def load_tobii_glasses3_recordings(self):

        # Load the data for all participants (ps)
        nursing_session_dir = RAW_DATA_DIR / 'nurse_use_case'
        ps = pymmdt.utils.tobii.load_session_data(nursing_session_dir, verbose=True)
