import datetime
import pathlib
import tempfile
import uuid

import numpy as np
import pytest

import chimerapy.engine as cpe
from chimerapy.engine.eventbus import EventBus
from chimerapy.engine.node.record_service import RecordService
from chimerapy.engine.states import NodeState

logger = cpe._logger.getLogger("chimerapy-engine")


@pytest.fixture
async def recorder():

    # Event Loop
    eventbus = EventBus()

    # Create sample state
    state = NodeState(logdir=pathlib.Path(tempfile.mkdtemp()))
    state.fsm = "PREVIEWING"

    # Create the recorder
    recorder = RecordService(name="recorder", state=state, eventbus=eventbus)
    await recorder.async_init()
    yield recorder
    await recorder.teardown()


async def test_instanciate(recorder):
    ...


async def test_record_direct_submit(recorder):

    # Run the recorder
    await recorder.setup()

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
    await recorder.teardown()

    expected_file = recorder.state.logdir / "test.mp4"
    assert expected_file.exists()
