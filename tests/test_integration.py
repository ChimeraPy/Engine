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
ROOT_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = CURRENT_DIR / 'data' 
OUTPUT_DIR = CURRENT_DIR / 'test_output' 

sys.path.append(str(ROOT_DIR))

class CollectorRunnerSessionIntegrationTestCase(unittest.TestCase):
    ...

class App2BackendIntegrationTestCase(unittest.TestCase):
    ...
