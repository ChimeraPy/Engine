# Built-in Imports
import os
import pathlib
import uuid
import asyncio
import json

# Third-party
import pytest

# Internal Imports
import chimerapy.engine as cpe
from chimerapy.engine.records.json_record import JSONRecord
from chimerapy.engine.eventbus import EventBus, Event

from .data_nodes import JSONNode

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


async def test_node_save_json_stream(json_node):

    # Event Loop
    eventbus = EventBus()

    # Check that the image was created
    expected_jsonl_path = pathlib.Path(json_node.state.logdir) / "test.jsonl"
    try:
        os.rmdir(expected_jsonl_path.parent)
    except OSError:
        ...

    # Stream
    await json_node.arun(eventbus=eventbus)

    # Wait to generate files
    await eventbus.asend(Event("start"))
    logger.debug("Finish start")
    await eventbus.asend(Event("record"))
    logger.debug("Finish record")
    await asyncio.sleep(3)
    await eventbus.asend(Event("stop"))
    logger.debug("Finish stop")

    await json_node.ashutdown()

    # Check that the image was created
    assert expected_jsonl_path.exists()
