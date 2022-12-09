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
def tabular_node_step():

    # Create a node
    an = TabularNode(name="tn", debug="step")

    return an


@pytest.fixture
def tabular_node_stream():

    # Create a node
    an = TabularNode(name="tn", debug="stream")

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


def test_save_handler_tabular(save_handler_and_queue):

    # Check that the tabular was created
    expected_tabular_path = TEST_DATA_DIR / "test.csv"
    try:
        os.remove(expected_tabular_path)
    except FileNotFoundError:
        ...

    # Decoupling
    save_handler, save_queue = save_handler_and_queue

    # Write to tabular file
    for i in range(5):
        data = {"time": time.time(), "content": "HELLO"}
        tabular_chunk = {
            "uuid": uuid.uuid4(),
            "name": "test",
            "data": data,
            "dtype": "tabular",
        }
        save_queue.put(tabular_chunk)

    # Shutdown save handler
    save_handler.shutdown()
    save_handler.join()

    # Check that the tabular was created
    expected_tabular_path = save_handler.logdir / "test.csv"
    assert expected_tabular_path.exists()


def test_node_save_tabular_single_step(tabular_node_step):

    # Check that the tabular was created
    expected_tabular_path = tabular_node_step.logdir / "test.csv"
    try:
        os.remove(expected_tabular_path)
    except FileNotFoundError:
        ...

    # Write to tabular file
    for i in range(5):
        tabular_node_step.step()

    # Stop the node the ensure tabular completion
    tabular_node_step.shutdown()
    time.sleep(1)

    # Check that the tabular was created
    assert expected_tabular_path.exists()


def test_node_save_tabular_stream(tabular_node_stream):

    # Check that the tabular was created
    expected_tabular_path = tabular_node_stream.logdir / "test.csv"
    try:
        os.remove(expected_tabular_path)
    except FileNotFoundError:
        ...

    # Stream
    tabular_node_stream.start()

    # Wait to generate files
    time.sleep(10)

    tabular_node_stream.shutdown()
    tabular_node_stream.join()

    # Check that the tabular was created
    assert expected_tabular_path.exists()
