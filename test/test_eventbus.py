from dataclasses import dataclass
from typing import List, Any

import pytest

from chimerapy.engine.eventbus import EventBus, TypedObserver, Event


@dataclass
class HelloEventData:
    message: str


@dataclass
class WorldEventData:
    value: int


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
