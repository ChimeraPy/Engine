# Built-in Imports
import os
import pathlib
import time
import datetime

# Third-party
import cv2
import numpy as np
import pytest
import chimerapy.engine as cpe
from chimerapy.engine.node.record_service.records.video_record import VideoRecord
from chimerapy.engine.node.record_service.entry import VideoEntry
from chimerapy.engine.node.events import RecordEvent
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.eventbus import EventBus, Event

from .data_nodes import VideoNode

# Internal Imports
logger = cpe._logger.getLogger("chimerapy-engine")

# cpe.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


@pytest.fixture
def video_node(logreceiver):

    # Create a node
    vn = VideoNode(name="vn", debug_port=logreceiver.port)

    return vn


@pytest.fixture
def eventbus():
    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)
    return eventbus


def test_video_record():

    # Check that the video was created
    expected_video_path = TEST_DATA_DIR / "test.mp4"
    try:
        os.remove(expected_video_path)
    except FileNotFoundError:
        ...

    # Create the record
    start_time = datetime.datetime.now()
    vr = VideoRecord(dir=TEST_DATA_DIR, name="test", start_time=start_time)

    # Write to video file
    fps = 30
    for i in range(fps):
        data = np.random.rand(200, 300, 3) * 255
        video_entry = VideoEntry(
            name="test",
            data=data,
            fps=fps,
            timestamp=start_time + datetime.timedelta(seconds=(i / fps)),
        )
        vr.write(video_entry)

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
    start_time = datetime.datetime.now()
    vr = VideoRecord(dir=TEST_DATA_DIR, name="test", start_time=start_time)

    # Write to video file
    fps = 30
    actual_fps = 10
    rec_time = 5
    for i in range(rec_time * actual_fps):

        # But actually, we are getting frames at 20 fps
        data = np.random.rand(200, 300, 3) * 255
        video_entry = VideoEntry(
            name="test",
            data=data,
            fps=fps,
            timestamp=start_time + datetime.timedelta(seconds=(i / actual_fps)),
        )
        vr.write(video_entry)

    # Close file
    vr.close()

    # Check that the video was created
    assert expected_video_path.exists()

    # Check video attributes
    cap = cv2.VideoCapture(str(expected_video_path))
    num_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    expected_num_frames = fps * rec_time
    assert (num_frames - expected_num_frames) / expected_num_frames <= 0.02


def test_node_save_video_stream(video_node, eventbus):

    # Check that the video was created
    expected_video_path = video_node.state.logdir / "test" / "test.mp4"
    try:
        os.remove(expected_video_path)
    except FileNotFoundError:
        ...

    # Stream
    video_node.run(blocking=False, eventbus=eventbus)

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

    video_node.shutdown()

    # Check that the video was created
    assert expected_video_path.exists()
    cap = cv2.VideoCapture(str(expected_video_path))  # Checking if video is corrupted
    cap.release()


def test_node_save_video_stream_with_unstable_fps(video_node, eventbus):

    # Check that the video was created
    expected_video_path = video_node.state.logdir / "test" / "test.mp4"
    try:
        os.remove(expected_video_path)
    except FileNotFoundError:
        ...

    # Video parameters (located within the VideoNode)
    fps = 30  # actual at 1/10
    rec_time = 10

    # Stream
    video_node.run(blocking=False, eventbus=eventbus)

    # Wait to generate files
    eventbus.send(Event("start")).result()
    logger.debug("Finish start")
    eventbus.send(Event("record", RecordEvent("test"))).result()
    logger.debug("Finish record")
    time.sleep(rec_time)
    eventbus.send(Event("stop")).result()
    logger.debug("Finish stop")
    eventbus.send(Event("collect")).result()
    logger.debug("Finish collect")

    video_node.shutdown()

    # Check that the video was created
    assert expected_video_path.exists()

    # Check video attributes
    cap = cv2.VideoCapture(str(expected_video_path))
    num_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    expected_num_frames = fps * rec_time
    assert (num_frames - expected_num_frames) / expected_num_frames <= 0.10  # 10% error
