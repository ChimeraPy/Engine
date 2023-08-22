# Built-in Imports
import datetime
import os
import pathlib
import time

# Third-party
import pytest
import chimerapy.engine as cpe
from chimerapy.engine.node.record_service.records.tabular_record import TabularRecord
from chimerapy.engine.node.record_service.entry import TabularEntry
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.node.events import RecordEvent
from chimerapy.engine.eventbus import EventBus, Event

from .data_nodes import TabularNode

# Internal Imports
logger = cpe._logger.getLogger("chimerapy-engine")

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
    tr = TabularRecord(
        dir=TEST_DATA_DIR, name="test", start_time=datetime.datetime.now()
    )

    # Write to tabular file
    for i in range(5):
        data = {"time": time.time(), "content": "HELLO"}
        tabular_entry = TabularEntry(name="test", data=data)
        tr.write(tabular_entry)

    assert expected_tabular_path.exists()


def test_node_save_tabular_stream(tabular_node):

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Check that the tabular was created
    expected_tabular_path = tabular_node.state.logdir / "test" / "test.csv"
    try:
        os.remove(expected_tabular_path)
    except FileNotFoundError:
        ...

    # Stream
    tabular_node.run(blocking=False, eventbus=eventbus)

    # Wait to generate files
    eventbus.send(Event("start")).result()
    logger.debug("Finish start")
    eventbus.send(Event("record", RecordEvent("test"))).result()
    logger.debug("Finish record")
    time.sleep(3)
    eventbus.send(Event("stop")).result()
    logger.debug("Finish stop")
    eventbus.send(Event("collect")).result()
    logger.debug("Finish collect")

    tabular_node.shutdown()

    # Check that the tabular was created
    assert expected_tabular_path.exists()
