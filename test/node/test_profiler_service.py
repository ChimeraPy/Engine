import pathlib
import random
import tempfile

import numpy as np
import pytest

import chimerapy.engine as cpe
from chimerapy.engine import config
from chimerapy.engine.networking.data_chunk import DataChunk
from chimerapy.engine.node.profiler_service import ProfilerService
from chimerapy.engine.states import NodeState

logger = cpe._logger.getLogger("chimerapy-engine")


@pytest.fixture
async def profiler(bus):

    # Modify the configuration
    config.set("diagnostics.interval", 1)
    config.set("diagnostics.logging-enabled", True)

    # Create sample state
    state = NodeState(logdir=pathlib.Path(tempfile.mkdtemp()))

    # Create the profiler
    profiler = ProfilerService(name="profiler", state=state, logger=logger)
    await profiler.attach(bus)
    await profiler.setup()
    yield profiler
    await profiler.teardown()


async def test_instanciate(profiler):
    ...


async def test_single_data_chunk(profiler, entrypoint):
    await profiler.enable()

    for _ in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["delta"] = random.randrange(500, 1500, 1)  # ms
        example_data_chunk.update("meta", meta)

        await entrypoint.emit("out_step", example_data_chunk)

    await profiler.diagnostics_report()
    assert profiler.log_file.exists()


async def test_single_data_chunk_with_multiple_payloads(profiler, entrypoint):
    await profiler.enable()

    for _ in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))
        example_data_chunk.add("random2", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["delta"] = random.randrange(500, 1500, 1)
        example_data_chunk.update("meta", meta)

        await entrypoint.emit("out_step", example_data_chunk)

    await profiler.diagnostics_report()
    assert profiler.log_file.exists()


async def test_enable_disable(profiler, entrypoint):

    for _ in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["delta"] = random.randrange(500, 1500, 1)  # ms
        example_data_chunk.update("meta", meta)

        await entrypoint.emit("out_step", example_data_chunk)

    assert len(profiler.seen_uuids) == 0
    await profiler.enable(True)

    for _ in range(50):

        # Run the step multiple times
        example_data_chunk = DataChunk()
        example_data_chunk.add("random", np.random.rand(1000, 1000, 3))

        # Mock how the processor marks the time when it got the datachunk
        # and transmitted it
        meta = example_data_chunk.get("meta")
        meta["value"]["delta"] = random.randrange(500, 1500, 1)  # ms
        example_data_chunk.update("meta", meta)

        await entrypoint.emit("out_step", example_data_chunk)

    await profiler.diagnostics_report()
    await profiler.enable(False)
    assert len(profiler.seen_uuids) != 0
    assert profiler.log_file.exists()
