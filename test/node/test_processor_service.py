import asyncio
import time
from typing import Dict

import pytest
from pytest_lazyfixture import lazy_fixture

from chimerapy.engine.node.processor_service import ProcessorService
from chimerapy.engine.networking.data_chunk import DataChunk
from chimerapy.engine.states import NodeState
from chimerapy.engine.eventbus import EventBus, Event, TypedObserver
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.node.events import NewInBoundDataEvent, NewOutBoundDataEvent
from chimerapy.engine import _logger


logger = _logger.getLogger("chimerapy-engine")

# Global
CHANGE_FLAG = False
RECEIVE_FLAG = False


@pytest.fixture
def eventbus():

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)
    return eventbus


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


async def emit_data(eventbus):
    for i in range(3):
        await eventbus.asend(
            Event("in_step", NewInBoundDataEvent({"data": DataChunk()}))
        )
        await asyncio.sleep(0.5)


async def receive_data(data_chunk):
    global RECEIVE_FLAG
    RECEIVE_FLAG = True
    logger.debug(data_chunk)


@pytest.fixture
def step_processor():

    # Create eventbus
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Create sample state
    state = NodeState()

    # Create the service
    processor = ProcessorService(
        "processor",
        in_bound_data=True,
        state=state,
        eventbus=eventbus,
        main_fn=step,
        operation_mode="step",
    )

    yield (processor, eventbus)

    thread.exec(processor.teardown()).result(timeout=10)


@pytest.fixture
def source_processor():

    # Create eventbus
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Create sample state
    state = NodeState()

    # Create the service
    processor = ProcessorService(
        "processor",
        in_bound_data=False,
        state=state,
        eventbus=eventbus,
        main_fn=step,
        operation_mode="step",
    )

    yield (processor, eventbus)

    thread.exec(processor.teardown()).result(timeout=10)


@pytest.fixture
def main_processor():

    # Create eventbus
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    # Create sample state
    state = NodeState()

    # Create the service
    processor = ProcessorService(
        "processor",
        in_bound_data=True,
        state=state,
        eventbus=eventbus,
        main_fn=main,
        operation_mode="main",
    )

    yield (processor, eventbus)

    eventbus.send(Event("teardown")).result(timeout=10)


@pytest.mark.parametrize(
    "processor_setup",
    [
        lazy_fixture("source_processor"),
        lazy_fixture("main_processor"),
        lazy_fixture("step_processor"),
    ],
)
def test_instanticate(processor_setup):
    ...


@pytest.mark.parametrize(
    "processor_setup",
    [
        lazy_fixture("source_processor"),
        lazy_fixture("main_processor"),
        lazy_fixture("step_processor"),
    ],
)
async def test_setup(processor_setup):
    processor, _ = processor_setup
    await processor.setup()


@pytest.mark.parametrize(
    "ptype, processor_setup",
    [
        ("source", lazy_fixture("source_processor")),
        ("main", lazy_fixture("main_processor")),
        ("step", lazy_fixture("step_processor")),
    ],
)
async def test_main(ptype, processor_setup):
    processor, eventbus = processor_setup

    # Reset
    global CHANGE_FLAG
    global RECEIVE_FLAG
    CHANGE_FLAG = False
    RECEIVE_FLAG = False

    # Adding observer for step
    if ptype == "step":
        observer = TypedObserver(
            "out_step",
            NewOutBoundDataEvent,
            on_asend=receive_data,
            handle_event="unpack",
        )
        await eventbus.asubscribe(observer)

    # Execute
    await processor.setup()
    await asyncio.gather(
        processor.main(),
        shutdown(processor),
        emit_data(eventbus),
    )

    # Asserts
    assert CHANGE_FLAG
    if ptype == "step":
        assert RECEIVE_FLAG
