import pathlib
import tempfile
import random

import numpy as np
import pytest

import chimerapy.engine as cpe
from chimerapy.engine import config
from chimerapy.engine.node.profiler_service import ProfilerService
from chimerapy.engine.node.events import NewOutBoundDataEvent
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


async def test_single_data_chunk(profiler_setup):
    profiler, eventbus = profiler_setup
    await profiler.enable()

    for i in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["delta"] = random.randrange(500, 1500, 1)  # ms
        example_data_chunk.update("meta", meta)

        await eventbus.asend(
            Event("out_step", NewOutBoundDataEvent(example_data_chunk))
        )

    await profiler.diagnostics_report()
    assert profiler.log_file.exists()


async def test_single_data_chunk_with_multiple_payloads(profiler_setup):
    profiler, eventbus = profiler_setup
    await profiler.enable()

    for i in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))
        example_data_chunk.add("random2", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["delta"] = random.randrange(500, 1500, 1)
        example_data_chunk.update("meta", meta)

        await eventbus.asend(
            Event("out_step", NewOutBoundDataEvent(example_data_chunk))
        )

    await profiler.diagnostics_report()
    assert profiler.log_file.exists()


async def test_enable_disable(profiler_setup):
    profiler, eventbus = profiler_setup

    for i in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["delta"] = random.randrange(500, 1500, 1)  # ms
        example_data_chunk.update("meta", meta)

        await eventbus.asend(
            Event("out_step", NewOutBoundDataEvent(example_data_chunk))
        )

    assert len(profiler.seen_uuids) == 0
    await profiler.enable(True)

    for i in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["delta"] = random.randrange(500, 1500, 1)  # ms
        example_data_chunk.update("meta", meta)

        await eventbus.asend(
            Event("out_step", NewOutBoundDataEvent(example_data_chunk))
        )

    await profiler.diagnostics_report()
    await profiler.enable(False)
    assert len(profiler.seen_uuids) != 0
    assert profiler.log_file.exists()
