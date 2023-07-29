import uuid
import asyncio
from datetime import datetime
from collections import deque
from concurrent.futures import Future
from typing import Any, Generic, Type, TypeVar, Callable, Awaitable, Optional, Literal

import dataclasses_json
from aioreactive import AsyncObservable, AsyncObserver, AsyncSubject
from dataclasses import dataclass, field, fields

from . import _logger
from .networking.async_loop_thread import AsyncLoopThread

logger = _logger.getLogger("chimerapy-engine")

T = TypeVar("T")

# Global variables
global_event_bus: Optional["EventBus"] = None


def configure(event_bus: "EventBus"):
    global global_event_bus
    global_event_bus = event_bus


def evented(cls):
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        global global_event_bus

        self.event_bus = None

        if isinstance(global_event_bus, EventBus):
            self.event_bus = global_event_bus

        original_init(self, *args, **kwargs)

    def make_property(name: str) -> Any:
        def getter(self):
            return self.__dict__[f"_{name}"]

        def setter(self, value):
            self.__dict__[f"_{name}"] = value
            if self.event_bus:
                event_name = f"{cls.__name__}.changed"
                event_data = DataClassEvent(self)
                self.event_bus.send(Event(event_name, event_data))

        return property(getter, setter)

    cls.__init__ = new_init

    for f in fields(cls):
        if f.name != "event_bus":
            setattr(cls, f.name, make_property(f.name))

    setattr(
        cls,
        "event_bus",
        dataclasses_json.config(
            field_name="event_bus", encoder=lambda x: None, decoder=lambda x: None
        ),
    )

    return cls


def make_evented(instance: T, event_bus: "EventBus") -> T:
    setattr(instance, "event_bus", event_bus)
    instance.__evented_values = {}  # type: ignore[attr-defined]

    # Dynamically create a new class with the same name as the instance's class
    new_class_name = instance.__class__.__name__
    NewClass = type(new_class_name, (instance.__class__,), {})

    def make_property(name: str):
        def getter(self):
            return self.__evented_values.get(name)

        def setter(self, value):
            self.__evented_values[name] = value
            event_name = f"{self.__class__.__name__}.changed"
            event_data = DataClassEvent(self)
            event_bus.send(Event(event_name, event_data))

        return property(getter, setter)

    for f in fields(instance.__class__):
        if f.name != "event_bus":
            instance.__evented_values[f.name] = getattr(  # type: ignore
                instance, f.name
            )
            setattr(NewClass, f.name, make_property(f.name))

    # Change the class of the instance
    instance.__class__ = NewClass

    return instance


@dataclass
class Event:
    type: str
    data: Optional[Any] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


@dataclass
class DataClassEvent:
    dataclass: Any


class EventBus(AsyncObservable):
    def __init__(self, thread: Optional[AsyncLoopThread] = None):
        self.stream = AsyncSubject()
        self._event_counts: int = 0
        self._sub_counts: int = 0
        self.thread = thread

    ####################################################################
    ## Async
    ####################################################################

    async def asend(self, event: Event):
        logger.debug(f"EventBus: Sending event: {event}")
        self._event_counts += 1
        await self.stream.asend(event)

    async def asubscribe(self, observer: AsyncObserver):
        self._sub_counts += 1
        await self.stream.subscribe_async(observer)

    ####################################################################
    ## Sync
    ####################################################################

    def send(self, event: Event) -> Future:
        assert isinstance(self.thread, AsyncLoopThread)
        return self.thread.exec(self.asend(event))

    def subscribe(self, observer: AsyncObserver):
        assert isinstance(self.thread, AsyncLoopThread)
        return self.thread.exec(self.asubscribe(observer))


class TypedObserver(AsyncObserver, Generic[T]):
    def __init__(
        self,
        event_type: str,
        event_data_cls: Optional[Type[T]] = None,
        on_asend: Optional[Callable] = None,
        on_athrow: Optional[Callable] = None,
        on_aclose: Optional[Callable] = None,
        handle_event: Literal["pass", "unpack", "drop"] = "pass",
    ):

        # Containers
        self.event_type = event_type
        self.event_data_cls = event_data_cls
        self.handle_event = handle_event
        self.received: deque[str] = deque(maxlen=10)

        # Callables
        self._on_asend = on_asend
        self._on_athrow = on_athrow
        self._on_aclose = on_aclose

    def __str__(self) -> str:
        string = f"<TypedObserver event_type={self.event_type}, "
        f"event_data_cls={self.event_data_cls}>"
        return string

    def bind_asend(self, func: Callable[[Event], Awaitable[None]]):
        self._on_asend = func

    def bind_athrow(self, func: Callable[[Exception], Awaitable[None]]):
        self._on_athrow = func

    def bind_aclose(self, func: Callable[[], Awaitable[None]]):
        self._on_aclose = func

    async def exec_callable(self, func: Callable, *arg, **kwargs):
        if asyncio.iscoroutinefunction(func):
            await func(*arg, **kwargs)
        else:
            func(*arg, **kwargs)

    async def asend(self, event: Event):

        if self.event_data_cls is None:
            is_match = event.type == self.event_type
        else:
            is_match = (
                isinstance(event.data, self.event_data_cls)
                and event.type == self.event_type
            )

        if is_match:
            # logger.debug(f"{self}: asend!")
            self.received.append(event.id)
            if self._on_asend:
                if self.handle_event == "pass":
                    await self.exec_callable(self._on_asend, event)
                elif self.handle_event == "unpack":
                    await self.exec_callable(self._on_asend, **event.data.__dict__)
                elif self.handle_event == "drop":
                    await self.exec_callable(self._on_asend)

    async def athrow(self, ex: Exception):
        if self._on_athrow:
            await self.exec_callable(self._on_athrow, ex)

    async def aclose(self):
        if self._on_aclose:
            await self.exec_callable(self._on_aclose)
