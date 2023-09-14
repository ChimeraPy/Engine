import datetime
import uuid
import pathlib
import tempfile

import pytest
import numpy as np

import chimerapy.engine as cpe
from chimerapy.engine.node.record_service import RecordService
from chimerapy.engine.states import NodeState
from chimerapy.engine.eventbus import EventBus, Event
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread


logger = cpe._logger.getLogger("chimerapy-engine")


@pytest.fixture
def recorder():

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Create sample state
    state = NodeState(logdir=pathlib.Path(tempfile.mkdtemp()))
    state.fsm = "PREVIEWING"

    # Create the recorder
    recorder = RecordService(name="recorder", state=state, eventbus=eventbus)

    yield recorder

    eventbus.send(Event("teardown")).result(timeout=10)


def test_instanciate(recorder):
    ...


async def test_record_direct_submit(recorder):

    # Run the recorder
    recorder.setup()

    timestamp = datetime.datetime.now()
    video_entry = {
        "uuid": uuid.uuid4(),
        "name": "test",
        "data": np.ndarray([255, 255]),
        "dtype": "video",
        "fps": 30,
        "elapsed": 0,
        "timestamp": timestamp,
    }

    for _ in range(50):
        recorder.submit(video_entry)

    recorder.collect()
    recorder.teardown()

    expected_file = recorder.state.logdir / "test.mp4"
    assert expected_file.exists()
