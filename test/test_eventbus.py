import asyncio
from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin
from typing import List, Any

import pytest

import chimerapy.engine as cpe
from chimerapy.engine.eventbus import (
    EventBus,
    TypedObserver,
    Event,
    evented,
    configure,
    make_evented,
)
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.states import ManagerState, WorkerState, NodeState

logger = cpe._logger.getLogger("chimerapy-engine")


@dataclass
class HelloEventData:
    message: str


@dataclass
class WorldEventData:
    value: int


@evented
@dataclass
class SomeClass(DataClassJsonMixin):
    number: int
    string: str


@pytest.fixture
def event_bus():
    # Creating the configuration for the eventbus and dataclasses
    thread = AsyncLoopThread()
    thread.start()
    event_bus = EventBus(thread=thread)
    configure(event_bus)
    return event_bus


@pytest.mark.asyncio
async def test_msg_filtering():

    event_bus = EventBus()
    hello_observer = TypedObserver("hello", HelloEventData)

    # Subscribe to the event bus
    await event_bus.asubscribe(hello_observer)

    # Create the event
    hello_event = Event("hello", HelloEventData("Hello data"))
    world_event = Event("world", WorldEventData(42))

    # Send some events
    await event_bus.asend(hello_event)
    await event_bus.asend(world_event)

    assert world_event.id not in hello_observer.received
    assert hello_event.id in hello_observer.received


@pytest.mark.asyncio
async def test_event_null_data():

    event_bus = EventBus()
    null_observer = TypedObserver("null")

    # Subscribe to the event bus
    await event_bus.asubscribe(null_observer)

    # Create the event
    null_event = Event("null")
    null2_event = Event("null2")

    # Send some events
    await event_bus.asend(null_event)
    await event_bus.asend(null2_event)

    assert null2_event.id not in null_observer.received
    assert null_event.id in null_observer.received


@pytest.mark.asyncio
async def test_sync_and_async_binding():

    event_bus = EventBus()
    hello_observer = TypedObserver("hello", HelloEventData)
    goodbye_observer = TypedObserver("goodbye", HelloEventData)

    # Creating handler
    sync_local_variable: List = []
    async_local_variable: List = []

    def add_to(var: List[Any]):
        var.append(1)

    async def async_add_to(_):
        async_local_variable.append(1)

    hello_observer.bind_asend(lambda _: add_to(sync_local_variable))
    goodbye_observer.bind_asend(async_add_to)

    # Subscribe to the event bus
    await event_bus.asubscribe(hello_observer)
    await event_bus.asubscribe(goodbye_observer)

    # Create the event
    hello_event = Event("hello", HelloEventData("Hello data"))
    goodbye_event = Event("goodbye", HelloEventData("Hello data"))

    # Send some events
    await event_bus.asend(hello_event)
    await event_bus.asend(goodbye_event)

    # Confirm
    assert len(sync_local_variable) != 0
    assert len(async_local_variable) != 0


@pytest.mark.asyncio
async def test_event_handling():

    event_bus = EventBus()
    pass_observer = TypedObserver("hello", HelloEventData, handle_event="pass")
    unpack_observer = TypedObserver("hello", HelloEventData, handle_event="unpack")
    drop_observer = TypedObserver("hello", HelloEventData, handle_event="drop")
    obs = [pass_observer, unpack_observer, drop_observer]

    # Creating handler

    pass_variable: List = []

    async def pass_func(event):
        assert isinstance(event, Event)
        pass_variable.append(1)

    unpack_variable: List = []

    async def unpack_func(message: str):
        assert isinstance(message, str)
        unpack_variable.append(1)

    drop_variable: List = []

    async def drop_func():
        drop_variable.append(1)

    # Bind
    pass_observer.bind_asend(pass_func)
    unpack_observer.bind_asend(unpack_func)
    drop_observer.bind_asend(drop_func)

    # Subscribe to the event bus
    for ob in obs:
        await event_bus.asubscribe(ob)

    # Send some events
    await event_bus.asend(Event("hello", HelloEventData("Hello data")))

    # Confirm
    assert len(pass_variable) != 0
    assert len(unpack_variable) != 0
    assert len(drop_variable) != 0


@pytest.mark.asyncio
async def test_evented_dataclass(event_bus):

    # Creating the observer and its binding
    evented_observer = TypedObserver("SomeClass.changed")

    # Creating handler
    local_variable: List = []

    async def add_to(event):
        local_variable.append(1)

    evented_observer.bind_asend(add_to)

    # Subscribe to the event bus
    await event_bus.asubscribe(evented_observer)

    # Create the evented class
    data = SomeClass(number=1, string="hello")

    # Trigger an event by changing the class
    data.number = 2
    await asyncio.sleep(1)

    # Confirm
    assert len(local_variable) != 0
    assert isinstance(data.to_json(), str)


@pytest.mark.asyncio
async def test_evented_wrapper(event_bus):

    # Creating the observer and its binding
    evented_observer = TypedObserver("SomeClass.changed")

    # Creating handler
    local_variable: List = []

    async def add_to(event):
        local_variable.append(1)

    evented_observer.bind_asend(add_to)

    # Subscribe to the event bus
    await event_bus.asubscribe(evented_observer)

    # Create the evented class
    data = make_evented(SomeClass(number=1, string="hello"), event_bus=event_bus)

    # Trigger an event by changing the class
    logger.debug("Triggering manually")
    data.number = 2
    await asyncio.sleep(1)

    # Confirm
    assert len(local_variable) != 0
    assert isinstance(data.to_json(), str)


@pytest.mark.parametrize(
    "cls, kwargs",
    [
        (SomeClass, {"number": 1, "string": "hello"}),
        (ManagerState, {}),
        (WorkerState, {"id": "test", "name": "test"}),
        (NodeState, {"id": "a"}),
    ],
)
def test_make_evented(cls, kwargs):
    # Create the evented class
    data = make_evented(cls(**kwargs), event_bus=event_bus)
    data.to_json()


def test_make_evented_multiple():
    # Create the evented class
    make_evented(SomeClass(number=1, string="hello"), event_bus=event_bus)
    make_evented(SomeClass(number=1, string="hello"), event_bus=event_bus)
    make_evented(SomeClass(number=1, string="hello"), event_bus=event_bus)