from .data_nodes import TextNode

# Built-in Imports
import os
import pathlib
import time
import uuid

# Third-party
import pytest

# Internal Imports
import chimerapy.engine as cpe
from chimerapy.engine.records.text_record import TextRecord
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.eventbus import EventBus, Event

logger = cpe._logger.getLogger("chimerapy-engine")

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


@pytest.fixture
def text_node():

    # Create a node
    text_n = TextNode(name="text_n", logdir=TEST_DATA_DIR)

    return text_n


def test_text_record():

    # Check that the image was created
    expected_text_path = TEST_DATA_DIR / "test-5.log"
    try:
        os.rmdir(expected_text_path.parent)
    except OSError:
        ...

    # Create the record
    text_r = TextRecord(dir=TEST_DATA_DIR, name="test-5")

    data = [
        "Lorem ipsum dolor sit amet, consectetur adipiscing elit, "
        "sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.\n",
        "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi "
        "ut aliquip ex ea commodo consequat.\n",
    ]

    # Write to image file
    for i in range(5):
        print("\n".join(data))
        text_chunk = {
            "uuid": uuid.uuid4(),
            "name": "test-5",
            "suffix": "log",
            "data": "".join(data),
            "dtype": "text",
        }
        text_r.write(text_chunk)

    # Check that the image was created
    assert expected_text_path.exists()

    with expected_text_path.open("r") as jlf:
        for idx, line in enumerate(jlf):
            assert line.strip() == (data[idx % len(data)]).strip()


def test_node_save_text_stream(text_node):

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Check that the image was created
    expected_text_path = pathlib.Path(text_node.state.logdir) / "test.text"
    try:
        os.rmdir(expected_text_path.parent)
    except OSError:
        ...

    # Stream
    text_node.run(blocking=False, eventbus=eventbus)

    # Wait to generate files
    eventbus.send(Event("start")).result()
    logger.debug("Finish start")
    eventbus.send(Event("record")).result()
    logger.debug("Finish record")
    time.sleep(3)
    eventbus.send(Event("stop")).result()
    logger.debug("Finish stop")

    text_node.shutdown()

    # Check that the image was created
    assert expected_text_path.exists()
