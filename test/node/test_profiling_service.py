import datetime
import time
import pathlib
import tempfile

import numpy as np
import pytest

import chimerapy.engine as cpe
from chimerapy.engine import config
from chimerapy.engine.node.profiler_service import ProfilerService
from chimerapy.engine.node.events import NewInBoundDataEvent
from chimerapy.engine.states import NodeState
from chimerapy.engine.eventbus import EventBus, Event
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.networking.data_chunk import DataChunk

# from ..conftest import TEST_DATA_DIR


logger = cpe._logger.getLogger("chimerapy-engine")


@pytest.fixture
def profiler_setup():

    # Modify the configuration
    config.set("diagnostics.interval", 1)
    config.set("diagnostics.logging-enabled", True)

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Create sample state
    # state = NodeState(logdir=TEST_DATA_DIR)
    state = NodeState(logdir=pathlib.Path(tempfile.mkdtemp()))

    # Create the profiler
    profiler = ProfilerService(
        name="profiler", state=state, eventbus=eventbus, logger=logger
    )
    eventbus.send(Event("setup")).result(timeout=10)

    yield (profiler, eventbus)

    eventbus.send(Event("teardown")).result(timeout=10)


def test_instanciate(profiler_setup):
    ...


def test_single_data_chunk(profiler_setup):
    profiler, eventbus = profiler_setup

    for i in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["transmitted"] = datetime.datetime.now()
        example_data_chunk.update("meta", meta)

        # Transmission time
        time.sleep(0.1)

        # Modify the received timestamp to mock the Poller
        meta = example_data_chunk.get("meta")
        meta["value"]["received"] = datetime.datetime.now()
        example_data_chunk.update("meta", meta)

        dcs = {"test": example_data_chunk}

        eventbus.send(Event("in_step", NewInBoundDataEvent(dcs))).result()

    assert profiler.log_file.exists()


def test_single_data_chunk_with_multiple_payloads(profiler_setup):
    profiler, eventbus = profiler_setup

    for i in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))
        example_data_chunk.add("random2", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["transmitted"] = datetime.datetime.now()
        example_data_chunk.update("meta", meta)

        # Transmission time
        time.sleep(0.1)

        # Modify the received timestamp to mock the Poller
        meta = example_data_chunk.get("meta")
        meta["value"]["received"] = datetime.datetime.now()
        example_data_chunk.update("meta", meta)

        dcs = {"test": example_data_chunk}

        eventbus.send(Event("in_step", NewInBoundDataEvent(dcs))).result()

    assert profiler.log_file.exists()


def test_multiple_data_chunk(profiler_setup):
    profiler, eventbus = profiler_setup

    for i in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk2 = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))
        example_data_chunk2.add("random", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["transmitted"] = datetime.datetime.now()
        example_data_chunk.update("meta", meta)

        meta = example_data_chunk2.get("meta")
        meta["value"]["transmitted"] = datetime.datetime.now()
        example_data_chunk2.update("meta", meta)

        # Transmission time
        time.sleep(0.1)

        # Modify the received timestamp to mock the Poller
        meta = example_data_chunk.get("meta")
        meta["value"]["received"] = datetime.datetime.now()
        example_data_chunk.update("meta", meta)

        meta = example_data_chunk2.get("meta")
        meta["value"]["received"] = datetime.datetime.now()
        example_data_chunk2.update("meta", meta)

        dcs = {"test": example_data_chunk, "test2": example_data_chunk2}

        eventbus.send(Event("in_step", NewInBoundDataEvent(dcs))).result()

    assert profiler.log_file.exists()
