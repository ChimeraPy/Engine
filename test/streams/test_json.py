from .data_nodes import JSONNode
import json

# Built-in Imports
import os
import pathlib
import time
import uuid

# Third-party
import pytest

# Internal Imports
import chimerapy.engine as cpe
from chimerapy.engine.records.json_record import JSONRecord
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.eventbus import EventBus, Event

logger = cpe._logger.getLogger("chimerapy-engine")

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


@pytest.fixture
def json_node():

    # Create a node
    json_n = JSONNode(name="img_n", logdir=TEST_DATA_DIR)

    return json_n


def test_image_record():

    # Check that the image was created
    expected_jsonl_path = TEST_DATA_DIR / "test-5.jsonl"
    try:
        os.rmdir(expected_jsonl_path.parent)
    except OSError:
        ...

    # Create the record
    json_r = JSONRecord(dir=TEST_DATA_DIR, name="test-5")

    data = {
        "content": "application/json",
        "response": 2,
        "count": 20,
        "next": "http://swapi.dev/api/people/?page=2",
        "previous": None,
        "results": [
            {
                "name": "Luke Skywalker",
                "height": "172",
                "mass": "77",
                "hair_color": "blond",
            },
            {
                "name": "C-3PO",
                "height": "167",
                "mass": "75",
                "hair_color": "n/a",
            },
        ],
    }

    # Write to image file
    for i in range(5):

        json_chunk = {
            "uuid": uuid.uuid4(),
            "name": "test",
            "data": data,
            "dtype": "json",
        }
        json_r.write(json_chunk)

    # Check that the image was created
    assert expected_jsonl_path.exists()

    with expected_jsonl_path.open("r") as jlf:
        for line in jlf:
            data_cp = json.loads(line)
            assert data_cp == data


def test_node_save_json_stream(json_node):

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Check that the image was created
    expected_jsonl_path = pathlib.Path(json_node.state.logdir) / "test.jsonl"
    try:
        os.rmdir(expected_jsonl_path.parent)
    except OSError:
        ...

    # Stream
    json_node.run(blocking=False, eventbus=eventbus)

    # Wait to generate files
    eventbus.send(Event("start")).result()
    logger.debug("Finish start")
    eventbus.send(Event("record")).result()
    logger.debug("Finish record")
    time.sleep(3)
    eventbus.send(Event("stop")).result()
    logger.debug("Finish stop")

    json_node.shutdown()

    # Check that the image was created
    assert expected_jsonl_path.exists()
