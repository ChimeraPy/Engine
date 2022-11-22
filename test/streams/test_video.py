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

logger = logging.getLogger("chimerapy")

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


class VideoNode(cp.Node):
    def step(self):
        time.sleep(1 / 30)
        rand_frame = np.random.rand(200, 300, 3) * 255
        self.save_video(name="test", data=rand_frame, fps=30)


@pytest.fixture
def save_handler_and_queue():

    save_queue = queue.Queue()
    save_handler = cp.SaveHandler(logdir=TEST_DATA_DIR, save_queue=save_queue)
    save_handler.start()

    return (save_handler, save_queue)


@pytest.fixture
def video_node():

    # Create a node
    vn = VideoNode(name="vn")
    vn.config("", 9000, TEST_DATA_DIR, [], [], follow=None, networking=False)
    vn._prep()

    return vn


def test_video_record():

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
    expected_video_path = vr.dir / "test.mp4"
    assert expected_video_path.exists()


def test_save_handler_video(save_handler_and_queue):

    # Decoupling
    save_handler, save_queue = save_handler_and_queue

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
    expected_video_path = save_handler.logdir / "test.mp4"
    assert expected_video_path.exists()


def test_node_save_video_single_step(video_node):

    fps = 30
    for i in range(fps * 5):
        video_node.step()

    # Stop the node the ensure video completion
    video_node.shutdown()
    video_node._teardown()

    # Check that the video was created
    expected_video_path = video_node.logdir / "test.mp4"
    assert expected_video_path.exists()


def test_node_save_video_stream(video_node):

    # Stream
    video_node.start()

    # Wait to generate files
    time.sleep(10)

    video_node.shutdown()
    video_node._teardown()
    video_node.join()

    # Check that the video was created
    expected_video_path = video_node.logdir / "test.mp4"
    assert expected_video_path.exists()
