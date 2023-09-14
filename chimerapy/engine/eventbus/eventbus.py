import uuid
import asyncio
from datetime import datetime
from collections import deque
from concurrent.futures import Future
from typing import (
    Any,
    Generic,
    Type,
    Callable,
    Awaitable,
    Optional,
    Literal,
    TypeVar,
    Dict,
)

from aioreactive import AsyncObservable, AsyncObserver, AsyncSubject
from dataclasses import dataclass, field

from .. import _logger
from ..networking.async_loop_thread import AsyncLoopThread

T = TypeVar("T")

logger = _logger.getLogger("chimerapy-engine")


@dataclass
class Event:
    type: str
    data: Optional[Any] = None
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())


class EventBus(AsyncObservable):
    def __init__(self, thread: Optional[AsyncLoopThread] = None):
        self.stream = AsyncSubject()
        self._event_counts: int = 0
        self._sub_counts: int = 0
        self.thread = thread

        # State information
        self.awaitable_events: Dict[str, asyncio.Event] = {}
        self.subscription_map: Dict[AsyncObserver, Any] = {}

    ####################################################################
    ## Async
    ####################################################################

    async def asend(self, event: Event):
        # logger.debug(f"EventBus: Sending event: {event}")
        self._event_counts += 1
        await self.stream.asend(event)

        if event.type in self.awaitable_events:
            self.latest_event = event
            self.awaitable_events[event.type].set()
            del self.awaitable_events[event.type]

    async def asubscribe(self, observer: AsyncObserver):
        self._sub_counts += 1
        subscription = await self.stream.subscribe_async(observer)
        self.subscription_map[observer] = subscription

    async def aunsubscribe(self, observer: AsyncObserver):
        if observer not in self.subscription_map:
            raise RuntimeError(
                "Trying to unsubscribe an Observer that is not subscribed"
            )

        self._sub_counts -= 1
        subscription = self.subscription_map[observer]
        await subscription.dispose_async()

    async def await_event(self, event_type: str) -> Event:
        if event_type not in self.awaitable_events:
            self.awaitable_events[event_type] = asyncio.Event()

        event_trigger = self.awaitable_events[event_type]
        await event_trigger.wait()
        return self.latest_event

    ####################################################################
    ## Sync
    ####################################################################

    def send(self, event: Event) -> Future:
        assert isinstance(self.thread, AsyncLoopThread)
        return self.thread.exec(self.asend(event))

    def subscribe(self, observer: AsyncObserver) -> Future:
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
