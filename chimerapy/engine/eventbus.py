import uuid
import asyncio
from collections import deque
from typing import Any, Generic, Type, TypeVar, Callable, Awaitable, Optional

from aioreactive import AsyncObservable, AsyncObserver, AsyncSubject
from dataclasses import dataclass, field

T = TypeVar("T")


@dataclass
class Event:
    type: str
    data: Any
    id: str = field(default_factory=lambda: str(uuid.uuid4()))


class EventBus(AsyncObservable):
    def __init__(self):
        self.stream = AsyncSubject()

    async def asend(self, value: Event):
        await self.stream.asend(value)

    async def subscribe(self, observer: AsyncObserver):
        await self.stream.subscribe_async(observer)


class TypedObserver(AsyncObserver, Generic[T]):
    def __init__(self, event_type: str, event_data_cls: Type[T]):

        # Containers
        self.event_type = event_type
        self.event_data_cls = event_data_cls
        self.received: deque[str] = deque(maxlen=10)

        # Callables
        self._on_asend: Optional[Callable] = None
        self._on_athrow: Optional[Callable] = None
        self._on_aclose: Optional[Callable] = None

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
        if (
            isinstance(event.data, self.event_data_cls)
            and event.type == self.event_type
        ):
            self.received.append(event.id)
            if self._on_asend:
                await self.exec_callable(self._on_asend, event)
            else:
                print(f"Received {self.event_type} event with value: {event.data}")

    async def athrow(self, ex: Exception):
        if self._on_athrow:
            await self.exec_callable(self._on_athrow, ex)
        else:
            print(f"Received exception: {ex}")

    async def aclose(self):
        if self._on_aclose:
            await self.exec_callable(self._on_aclose)
        else:
            print("Completed")
