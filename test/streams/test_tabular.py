# Built-in Imports
import asyncio
import os
import pathlib
import time
import uuid

# Third-party
import pytest

import chimerapy.engine as cpe
from chimerapy.engine.eventbus import Event, EventBus
from chimerapy.engine.records.tabular_record import TabularRecord

from .data_nodes import TabularNode

# Internal Imports
logger = cpe._logger.getLogger("chimerapy-engine")

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


@pytest.fixture
def tabular_node():

    # Create a node
    an = TabularNode(name="tn", logdir=TEST_DATA_DIR)

    return an


def test_tabular_record():

    # Check that the tabular was created
    expected_tabular_path = TEST_DATA_DIR / "test.csv"
    try:
        os.remove(expected_tabular_path)
    except FileNotFoundError:
        ...

    # Create the record
    tr = TabularRecord(dir=TEST_DATA_DIR, name="test")

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


async def test_node_save_tabular_stream(tabular_node):

    # Event Loop
    eventbus = EventBus()

    # Check that the tabular was created
    expected_tabular_path = pathlib.Path(tabular_node.state.logdir) / "test.csv"
    try:
        os.remove(expected_tabular_path)
    except FileNotFoundError:
        ...

    # Stream
    await tabular_node.arun(eventbus=eventbus)

    # Wait to generate files
    await eventbus.asend(Event("start"))
    logger.debug("Finish start")
    await eventbus.asend(Event("record"))
    logger.debug("Finish record")
    await asyncio.sleep(3)
    await eventbus.asend(Event("stop"))
    logger.debug("Finish stop")

    await tabular_node.ashutdown()

    # Check that the tabular was created
    assert expected_tabular_path.exists()
