# Built-in Imports
import os
import pathlib
import platform
import tempfile
import logging
import time
import queue
import uuid

# Third-party
import numpy as np
import pytest
import chimerapy as cp

# Internal Imports
logger = cp._logger.getLogger("chimerapy")
from .data_nodes import VideoNode

# cp.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


@pytest.fixture
def video_node_step():

    # Create a node
    vn = VideoNode(name="vn", debug="step")

    return vn


@pytest.fixture
def video_node_stream():

    # Create a node
    vn = VideoNode(name="vn", debug="stream")

    return vn


def test_video_record():

    # Check that the video was created
    expected_video_path = TEST_DATA_DIR / "test.mp4"
    try:
        os.remove(expected_video_path)
    except FileNotFoundError:
        ...

    # Create the record
    vr = cp.records.VideoRecord(dir=TEST_DATA_DIR, name="test")

    # Write to video file
    fps = 30
    for i in range(fps):
        data = np.random.rand(200, 300, 3) * 255
        video_chunk = {
            "uuid": uuid.uuid4(),
            "name": "test",
            "data": data,
            "dtype": "video",
            "fps": fps,
        }
        vr.write(video_chunk)

    # Check that the video was created
    assert expected_video_path.exists()


def test_save_handler_video(save_handler_and_queue):

    # Decoupling
    save_handler, save_queue = save_handler_and_queue

    # Check that the video was created
    expected_video_path = TEST_DATA_DIR / "test.mp4"
    try:
        os.remove(expected_video_path)
    except FileNotFoundError:
        ...

    # Place multiple random video
    fps = 30
    for i in range(fps * 5):
        data = np.random.rand(200, 300, 3) * 255
        video_chunk = {
            "uuid": uuid.uuid4(),
            "name": "test",
            "data": data,
            "dtype": "video",
            "fps": fps,
        }

        save_queue.put(video_chunk)

    # Shutdown save handler
    save_handler.shutdown()
    save_handler.join()

    # Check that the video was created
    assert expected_video_path.exists()


def test_node_save_video_single_step(video_node_step, logreceiver):

    # Check that the video was created
    expected_video_path = video_node_step.logdir / "test.mp4"
    try:
        os.remove(expected_video_path)
    except FileNotFoundError:
        ...

    fps = 30
    for i in range(fps * 5):
        video_node_step.step()

    # Stop the node the ensure video completion
    video_node_step.shutdown()

    # Check that the video was created
    assert expected_video_path.exists()


def test_node_save_video_stream(video_node_stream, logreceiver):

    # Check that the video was created
    expected_video_path = video_node_stream.logdir / "test.mp4"
    try:
        os.remove(expected_video_path)
    except FileNotFoundError:
        ...

    # Stream
    video_node_stream.start()

    # Wait to generate files
    time.sleep(10)

    video_node_stream.shutdown()
    video_node_stream.join()

    # Check that the video was created
    assert expected_video_path.exists()
