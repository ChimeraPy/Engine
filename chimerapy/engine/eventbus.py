import uuid
import asyncio
from datetime import datetime
from collections import deque
from typing import Any, Generic, Type, TypeVar, Callable, Awaitable, Optional

from aioreactive import AsyncObservable, AsyncObserver, AsyncSubject
import dataclasses_json
from dataclasses import dataclass, field, fields

from . import _logger
from .networking.async_loop_thread import AsyncLoopThread

logger = _logger.getLogger("chimerapy-engine")

T = TypeVar("T")

# Global variables
global_event_bus: Optional["EventBus"] = None
global_thread: Optional[AsyncLoopThread] = None


def configure(event_bus: "EventBus", thread: AsyncLoopThread):
    global global_event_bus
    global global_thread
    global_event_bus = event_bus
    global_thread = thread


def evented(cls):
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        global global_event_bus
        global global_thread

        self.event_bus = None
        self.thread = None

        if isinstance(global_event_bus, EventBus):
            self.event_bus = global_event_bus
            self.thread = global_thread

        original_init(self, *args, **kwargs)

    def make_property(name: str) -> Any:
        def getter(self):
            return self.__dict__[f"_{name}"]

        def setter(self, value):
            self.__dict__[f"_{name}"] = value
            if self.event_bus and self.thread:
                event_name = f"{cls.__name__}.changed"
                event_data = DataClassEvent(self)
                self.thread.exec(self.event_bus.asend(Event(event_name, event_data)))

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
    def __init__(self):
        self.stream = AsyncSubject()
        self._event_counts: int = 0
        self._sub_counts: int = 0

    async def asend(self, event: Event):
        self._event_counts += 1
        await self.stream.asend(event)

    async def subscribe(self, observer: AsyncObserver):
        self._sub_counts += 1
        await self.stream.subscribe_async(observer)


class TypedObserver(AsyncObserver, Generic[T]):
    def __init__(
        self,
        event_type: str,
        event_data_cls: Optional[Type[T]] = None,
        on_asend: Optional[Callable] = None,
        on_athrow: Optional[Callable] = None,
        on_aclose: Optional[Callable] = None,
        drop_event: bool = False,
    ):

        # Containers
        self.event_type = event_type
        self.event_data_cls = event_data_cls
        self.drop_event = drop_event
        self.received: deque[str] = deque(maxlen=10)

        # Callables
        self._on_asend = on_asend
        self._on_athrow = on_athrow
        self._on_aclose = on_aclose

    def bind_asend(self, func: Callable[[Event], Awaitable[None]]):
        self._on_asend = func

    def bind_athrow(self, func: Callable[[Exception], Awaitable[None]]):
        self._on_athrow = func

    def bind_aclose(self, func: Callable[[], Awaitable[None]]):
        self._on_aclose = func

    async def exec_callable(self, func: Callable, *arg):
        if asyncio.iscoroutinefunction(func):
            await func(*arg)
        else:
            func(*arg)

    async def asend(self, event: Event):

        if self.event_data_cls is None:
            is_match = event.type == self.event_type
        else:
            is_match = (
                isinstance(event.data, self.event_data_cls)
                and event.type == self.event_type
            )
        if is_match:
            self.received.append(event.id)
            if self._on_asend:
                if self.drop_event:
                    await self.exec_callable(self._on_asend)
                else:
                    await self.exec_callable(self._on_asend, event)

    async def athrow(self, ex: Exception):
        if self._on_athrow:
            await self.exec_callable(self._on_athrow, ex)

    async def aclose(self):
        if self._on_aclose:
            await self.exec_callable(self._on_aclose)
