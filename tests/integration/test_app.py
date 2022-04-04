# Built-in Imports
import sys
import unittest
import pathlib
import os
import threading
import time

# Resources:
# https://stackoverflow.com/questions/29777494/use-one-single-instance-of-qapplication-in-python-unit-test

# Third-Party Imports

# Testing Library
import chimerapy.app as cpa

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output'

# Test input parameters

null_args = {
    "logdir": pathlib.Path('.'),
    "verbose": True
}

empty_dir_args = {
    "logdir": pathlib.Path(RAW_DATA_DIR / 'gui_input_data_case' / 'example_2'),
    "verbose": True
}
    
valid_complete_example_args = {
    "logdir": pathlib.Path(RAW_DATA_DIR / 'gui_input_data_case' / 'example_1'),
    "verbose": True
}

# Test Classes

# class TestNullTestCase(unittest.TestCase):

#     def setUp(self):
#         # Setup the application
#         self.app, self.engine, self.manager = mma.application_setup(null_args)

#     def test_correct_page(self):
#         assert self.manager._page == 'homePage'

# class TestEmptyLogdirTestCase(unittest.TestCase):

#     def setUp(self):
#         # Setup the application
#         self.app, self.engine, self.manager = mma.application_setup(empty_dir_args)

#     def test_correct_page(self):
#         assert self.manager._page == 'homePage'

# class TestDashboardAppTestCase(unittest.TestCase):
    
#     def setUp(self):
#         # Setup the application
#         self.app, self.engine, self.manager = mma.application_setup(valid_complete_example_args)

#     def test_correct_page(self):
#         assert self.manager._page == 'dashboardPage'

#     def test_correct_entries(self):
#         assert len(self.manager.entries) == 12

if __name__ == '__main__':
    unittest.main()
