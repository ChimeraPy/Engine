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
import cv2
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
def video_node_step(logreceiver):

    # Create a node
    vn = VideoNode(name="vn", debug="step", debug_port=logreceiver.port)

    return vn


@pytest.fixture
def video_node_stream(logreceiver):

    # Create a node
    vn = VideoNode(name="vn", debug="stream", debug_port=logreceiver.port)

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
            "timestamp": i / fps,
        }
        vr.write(video_chunk)

    # Close file
    vr.close()

    # Check that the video was created
    assert expected_video_path.exists()

    # Check video attributes
    cap = cv2.VideoCapture(str(expected_video_path))
    num_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    assert num_frames == fps


def test_video_record_with_unstable_frames():

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
    actual_fps = 10
    rec_time = 5
    for i in range(rec_time * actual_fps):

        # But actually, we are getting frames at 20 fps
        timestamp = i / actual_fps
        data = np.random.rand(200, 300, 3) * 255
        video_chunk = {
            "uuid": uuid.uuid4(),
            "name": "test",
            "data": data,
            "dtype": "video",
            "fps": fps,
            "timestamp": timestamp,
        }
        vr.write(video_chunk)

    # Close file
    vr.close()

    # Check that the video was created
    assert expected_video_path.exists()

    # Check video attributes
    cap = cv2.VideoCapture(str(expected_video_path))
    num_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    expected_num_frames = fps * rec_time
    assert (num_frames - expected_num_frames) / expected_num_frames <= 0.02


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
            "timestamp": i / fps,
        }

        save_queue.put(video_chunk)

    # Shutdown save handler
    save_handler.shutdown()
    save_handler.join()

    # Check that the video was created
    assert expected_video_path.exists()


def test_node_save_video_single_step(video_node_step):

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


def test_node_save_video_stream(video_node_stream):

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


def test_node_save_video_stream_with_unstable_fps(video_node_stream):

    # Check that the video was created
    expected_video_path = video_node_stream.logdir / "test.mp4"
    try:
        os.remove(expected_video_path)
    except FileNotFoundError:
        ...

    # Video parameters (located within the VideoNode)
    fps = 30  # actual at 1/10
    rec_time = 10

    # Stream
    video_node_stream.start()

    # Wait to generate files
    time.sleep(rec_time)

    video_node_stream.shutdown()
    video_node_stream.join()

    # Check that the video was created
    assert expected_video_path.exists()

    # Check video attributes
    cap = cv2.VideoCapture(str(expected_video_path))
    num_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    expected_num_frames = fps * rec_time
    assert (num_frames - expected_num_frames) / expected_num_frames <= 0.10  # 10% error
