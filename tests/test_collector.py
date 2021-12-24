
# Built-in Imports
import unittest
import pathlib
import shutil
import os
import sys
import time
import collections

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

class CollectorTestCase(unittest.TestCase):
    
    def setUp(self):

        # Load the data for all participants (ps)
        nursing_session_dir = RAW_DATA_DIR / 'nurse_use_case'
        ps = pymmdt.utils.tobii.load_session_data(nursing_session_dir, verbose=True)

        # Create a collector 
        self.collector = mm.Collector(
            {'P01': ps['P01']['data']},
            time_window_size=pd.Timedelta(seconds=3),
            max_queue_size=10,
            verbose=True
        )

    def test_start(self):
        print("Starting collector")
        self.collector.start()

        print("Waiting for two seconds")
        for i in tqdm.tqdm(range(2)):
            time.sleep(1)

        print("Closing collector")
        self.collector.close()

        # First, the queue size shouldn't be zero
        assert self.collector.loading_queue.qsize() != 0

        # Then check what is the generated data for the collector
        while self.collector.loading_queue.qsize() != 0:

            # Get data
            data = self.collector.loading_queue.get()

            # If last, we should get the message
            if self.collector.loading_queue.qsize() == 0:
                assert data == 'END'
            # Else, we should get the standard defaultdict
            else:
                assert isinstance(data, collections.defaultdict)

if __name__ == '__main__':
    # Run when debugging is not needed
    # unittest.main()

    # Otherwise, we have to call the tests ourselves
    test_case = CollectorTestCase()
    test_case.setUp()
    test_case.test_start()
