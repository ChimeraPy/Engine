import asyncio
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import List, Any

import pytest

import chimerapy.engine as cpe
from chimerapy.engine.eventbus import EventBus, TypedObserver, Event, evented, configure
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread

logger = cpe._logger.getLogger("chimerapy-engine")


@dataclass
class HelloEventData:
    message: str


@dataclass
class WorldEventData:
    value: int


@evented
@dataclass_json
@dataclass
class SomeClass:
    number: int
    string: str


@pytest.mark.asyncio
async def test_msg_filtering():

    event_bus = EventBus()
    hello_observer = TypedObserver("hello", HelloEventData)

    # Subscribe to the event bus
    await event_bus.subscribe(hello_observer)

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
    await event_bus.subscribe(null_observer)

    # Create the event
    null_event = Event("null")
    null2_event = Event("null2")

    # Send some events
    await event_bus.asend(null_event)
    await event_bus.asend(null2_event)

    assert null2_event.id not in null_observer.received
    assert null_event.id in null_observer.received


@pytest.mark.asyncio
async def test_sync_binding():

    event_bus = EventBus()
    hello_observer = TypedObserver("hello", HelloEventData)

    # Creating handler
    local_variable: List = []

    def add_to(var: List[Any]):
        var.append(1)

    hello_observer.bind_asend(lambda event: add_to(local_variable))

    # Subscribe to the event bus
    await event_bus.subscribe(hello_observer)

    # Create the event
    hello_event = Event("hello", HelloEventData("Hello data"))

    # Send some events
    await event_bus.asend(hello_event)

    # Confirm
    assert len(local_variable) != 0


@pytest.mark.asyncio
async def test_async_binding():

    event_bus = EventBus()
    hello_observer = TypedObserver("hello", HelloEventData)

    # Creating handler
    local_variable: List = []

    async def add_to(event):
        local_variable.append(1)

    hello_observer.bind_asend(add_to)

    # Subscribe to the event bus
    await event_bus.subscribe(hello_observer)

    # Create the event
    hello_event = Event("hello", HelloEventData("Hello data"))

    # Send some events
    await event_bus.asend(hello_event)

    # Confirm
    assert len(local_variable) != 0


@pytest.mark.asyncio
async def test_drop_event():

    event_bus = EventBus()
    hello_observer = TypedObserver("hello", HelloEventData, drop_event=True)

    # Creating handler
    local_variable: List = []

    async def add_to():
        local_variable.append(1)

    hello_observer.bind_asend(add_to)

    # Subscribe to the event bus
    await event_bus.subscribe(hello_observer)

    # Create the event
    hello_event = Event("hello", HelloEventData("Hello data"))

    # Send some events
    await event_bus.asend(hello_event)

    # Confirm
    assert len(local_variable) != 0


@pytest.mark.asyncio
async def test_evented_dataclass():

    # Creating the configuration for the eventbus and dataclasses
    event_bus = EventBus()
    thread = AsyncLoopThread()
    thread.start()
    configure(event_bus, thread)

    # Creating the observer and its binding
    evented_observer = TypedObserver("SomeClass.changed")

    # Creating handler
    local_variable: List = []

    async def add_to(event):
        local_variable.append(1)

    evented_observer.bind_asend(add_to)

    # Subscribe to the event bus
    await event_bus.subscribe(evented_observer)

    # Create the evented class
    data = SomeClass(number=1, string="hello")

    # Trigger an event by changing the class
    data.number = 2
    await asyncio.sleep(1)

    # Confirm
    assert len(local_variable) != 0
    assert isinstance(data.to_json(), str)
