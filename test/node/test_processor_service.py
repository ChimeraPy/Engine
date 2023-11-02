import asyncio
import time
from typing import Dict

import pytest
from aiodistbus import EntryPoint, EventBus
from pytest_lazyfixture import lazy_fixture

from chimerapy.engine import _logger
from chimerapy.engine.networking.data_chunk import DataChunk
from chimerapy.engine.node.events import NewInBoundDataEvent, NewOutBoundDataEvent
from chimerapy.engine.node.processor_service import ProcessorService
from chimerapy.engine.states import NodeState

logger = _logger.getLogger("chimerapy-engine")

# Global
CHANGE_FLAG = False
RECEIVE_FLAG = False


async def main():
    global CHANGE_FLAG
    CHANGE_FLAG = True
    await asyncio.sleep(1)
    logger.debug("End of main")


def step(data_chunks: Dict[str, DataChunk] = {}):
    global CHANGE_FLAG
    CHANGE_FLAG = True
    time.sleep(1)
    logger.debug("End of step")
    return 1


async def shutdown(processor):
    await asyncio.sleep(3)
    logger.debug("End of test")
    await processor.teardown()


async def emit_data(entrypoint):
    await asyncio.sleep(1)
    for _ in range(3):
        await entrypoint.emit("in_step", {"data": DataChunk()})
        await asyncio.sleep(0.5)


async def receive_data(data_chunk: DataChunk):
    global RECEIVE_FLAG
    RECEIVE_FLAG = True
    logger.debug(data_chunk)


@pytest.fixture
async def step_processor(bus):

    # Create sample state
    state = NodeState()

    # Create the service
    processor = ProcessorService(
        "processor",
        in_bound_data=True,
        state=state,
        main_fn=step,
        operation_mode="step",
    )
    await processor.attach(bus)
    yield processor
    await processor.teardown()


@pytest.fixture
async def source_processor(bus):

    # Create sample state
    state = NodeState()

    # Create the service
    processor = ProcessorService(
        "processor",
        in_bound_data=False,
        state=state,
        main_fn=step,
        operation_mode="step",
    )
    await processor.attach(bus)
    yield processor
    await processor.teardown()


@pytest.fixture
async def main_processor(bus):

    # Create sample state
    state = NodeState()

    # Create the service
    processor = ProcessorService(
        "processor",
        in_bound_data=True,
        state=state,
        main_fn=main,
        operation_mode="main",
    )
    await processor.attach(bus)
    yield processor
    await processor.teardown()


@pytest.mark.parametrize(
    "processor",
    [
        lazy_fixture("source_processor"),
        lazy_fixture("main_processor"),
        lazy_fixture("step_processor"),
    ],
)
def test_instanticate(processor):
    ...


@pytest.mark.parametrize(
    "processor",
    [
        lazy_fixture("source_processor"),
        lazy_fixture("main_processor"),
        lazy_fixture("step_processor"),
    ],
)
async def test_setup(processor):
    await processor.setup()


@pytest.mark.parametrize(
    "ptype, processor",
    [
        ("source", lazy_fixture("source_processor")),
        ("main", lazy_fixture("main_processor")),
        ("step", lazy_fixture("step_processor")),
    ],
)
async def test_main(ptype, processor, entrypoint):

    # Reset
    global CHANGE_FLAG
    global RECEIVE_FLAG
    CHANGE_FLAG = False
    RECEIVE_FLAG = False

    # Adding observer for step
    if ptype == "step":
        await entrypoint.on("out_step", receive_data, DataChunk)

    # Execute
    await processor.setup()
    await asyncio.gather(
        processor.main(),
        shutdown(processor),
        emit_data(entrypoint),
    )

    # Asserts
    assert CHANGE_FLAG
    if ptype == "step":
        assert RECEIVE_FLAG
