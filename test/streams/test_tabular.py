# Built-in Imports
import os
import pathlib
import logging
import uuid
import time

# Third-party
import pandas as pd
import numpy as np
import pytest
import chimerapy as cp

# Internal Imports
logger = cp._logger.getLogger("chimerapy")
from .data_nodes import TabularNode

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


@pytest.fixture
def tabular_node():

    # Create a node
    an = TabularNode(name="tn")

    return an


def test_tabular_record():

    # Check that the tabular was created
    expected_tabular_path = TEST_DATA_DIR / "test.csv"
    try:
        os.remove(expected_tabular_path)
    except FileNotFoundError:
        ...

    # Create the record
    tr = cp.records.TabularRecord(dir=TEST_DATA_DIR, name="test")

    # Write to tabular file
    for i in range(5):
        data = {"time": time.time(), "content": "HELLO"}
        tabular_chunk = {
            "uuid": uuid.uuid4(),
            "name": "test",
            "data": data,
            "dtype": "tabular",
        }
        tr.write(tabular_chunk)

    assert expected_tabular_path.exists()


def test_node_save_tabular_stream(tabular_node):

    # Check that the tabular was created
    expected_tabular_path = pathlib.Path(tabular_node.logdir) / "test.csv"
    try:
        os.remove(expected_tabular_path)
    except FileNotFoundError:
        ...

    # Stream
    tabular_node.run(blocking=False)

    # Wait to generate files
    time.sleep(3)

    tabular_node.shutdown()

    # Check that the tabular was created
    assert expected_tabular_path.exists()
