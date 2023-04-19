# Built-in Imports
import os
import pathlib
import logging
import time
import uuid

# Third-party
import numpy as np
import pytest

# Internal Imports
import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
from .data_nodes import ImageNode

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


@pytest.fixture
def image_node():

    # Create a node
    img_n = ImageNode(name="img_n")

    return img_n


def test_image_record():

    # Check that the image was created
    expected_image_path = TEST_DATA_DIR / "test" / "0.png"
    try:
        os.rmdir(expected_image_path.parent)
    except OSError:
        ...

    # Create the record
    img_r = cp.records.ImageRecord(dir=TEST_DATA_DIR, name="test")

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


def test_node_save_image_stream(image_node):

    # Check that the image was created
    expected_image_path = pathlib.Path(image_node.logdir) / "test" / "0.png"
    try:
        os.rmdir(expected_image_path.parent)
    except OSError:
        ...

    # Stream
    image_node.run(blocking=False)

    # Wait to generate files
    time.sleep(3)

    image_node.shutdown()

    # Check that the image was created
    assert expected_image_path.exists()
