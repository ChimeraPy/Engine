# Built-in Imports
import unittest
import pathlib

class BaseExampleTestCase(unittest.TestCase):

    def setUp(self):
        ...

class TestCaseOne(BaseExampleTestCase):

    def test_something(self):
        ...

if __name__ == "__main__":
    # Run when debugging is not needed
    # unittest.main()

    # Otherwise, we have to call the test ourselves
    test_case = TestCaseOne()
    test_case.test_something()
