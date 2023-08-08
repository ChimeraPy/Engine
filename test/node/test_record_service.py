import uuid
from typing import List

import pytest
import numpy as np

import chimerapy.engine as cpe
from chimerapy.engine.node.record_service import RecordService
from chimerapy.engine.node.record_service.recording import Recording
from chimerapy.engine.node.record_service.entry import VideoEntry
from chimerapy.engine.states import NodeState
from chimerapy.engine.eventbus import EventBus, Event
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread


logger = cpe._logger.getLogger("chimerapy-engine")


@pytest.fixture(scope="module")
def recorder():

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Create sample state
    state = NodeState()
    state.fsm = "PREVIEWING"

    # Create the recorder
    recorder = RecordService(name="recorder", state=state, eventbus=eventbus)

    yield recorder

    eventbus.send(Event("teardown")).result(timeout=10)


def test_instanciate(recorder):
    ...


def test_record_direct_submit(recorder):

    # Run the recorder
    recording = recorder.record(str(uuid.uuid4()))

    video_entry = VideoEntry(name="test", data=np.ndarray([255, 255, 3]))

    for _ in range(50):
        recorder.submit(video_entry)

    recorder.collect()

    expected_file = recording.dir / "test.mp4"
    assert expected_file.exists()


def test_multiple_recordings(recorder):

    N = 10

    recordings: List[Recording] = []
    for i in range(N):

        logger.debug(f"Starting recording {i}")

        # Run the recorder
        recording = recorder.record(str(uuid.uuid4()))
        recordings.append(recording)

        video_entry = VideoEntry(name="test", data=np.ndarray([255, 255, 3]))

        for _ in range(50):
            recorder.submit(video_entry)

        # Mark recordings to start stopping
        recorder.stop()

    # For all recordings to stop
    recorder.collect()

    for recording in recordings:

        expected_file = recording.dir / "test.mp4"
        assert expected_file.exists()
