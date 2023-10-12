# Built-in Imports
import asyncio
import os
import pathlib
import uuid

# Third-party
import numpy as np
import pytest

# Internal Imports
import chimerapy.engine as cpe
from chimerapy.engine.records.image_record import ImageRecord
from chimerapy.engine.eventbus import EventBus, Event

from .data_nodes import ImageNode

logger = cpe._logger.getLogger("chimerapy-engine")

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


@pytest.fixture
def image_node():

    # Create a node
    img_n = ImageNode(name="img_n", logdir=TEST_DATA_DIR)

    return img_n


def test_image_record():

    # Check that the image was created
    expected_image_path = TEST_DATA_DIR / "test" / "0.png"
    try:
        os.rmdir(expected_image_path.parent)
    except OSError:
        ...

    # Create the record
    img_r = ImageRecord(dir=TEST_DATA_DIR, name="test")

    # Write to image file
    for i in range(5):
        data = np.random.rand(200, 300, 3) * 255
        image_chunk = {
            "uuid": uuid.uuid4(),
            "name": "test",
            "data": data,
            "dtype": "image",
        }
        img_r.write(image_chunk)

    # Check that the image was created
    assert expected_image_path.exists()


async def test_node_save_image_stream(image_node):

    # Event Loop
    eventbus = EventBus()

    # Check that the image was created
    expected_image_path = pathlib.Path(image_node.state.logdir) / "test" / "0.png"
    try:
        os.rmdir(expected_image_path.parent)
    except OSError:
        ...

    # Stream
    await image_node.arun(eventbus=eventbus)

    # Wait to generate files
    await eventbus.asend(Event("start"))
    logger.debug("Finish start")
    await eventbus.asend(Event("record"))
    logger.debug("Finish record")
    await asyncio.sleep(3)
    await eventbus.asend(Event("stop"))
    logger.debug("Finish stop")

    await image_node.ashutdown()

    # Check that the image was created
    assert expected_image_path.exists()
