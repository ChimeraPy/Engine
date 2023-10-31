from abc import ABC, abstractmethod
from collections import UserDict
from typing import Any, Dict, List, Optional

from aiodistbus import EntryPoint, EventBus, registry


class Service(ABC):
    def __init__(self, name: str):
        self.name = name
        self.entrypoint: Optional[EntryPoint] = None

    def __str__(self):
        return f"<{self.__class__.__name__}, name={self.name}>"

    async def attach(self, bus: EventBus):
        """Attach the service to the bus.

        This is where the service should register its entrypoint and connect to the bus.

        Args:
            bus (EventBus): The bus to attach to.

        Raises:
            ValueError: If the registry is empty for service's Namespace.

        """
        self.entrypoint = EntryPoint()
        await self.entrypoint.use(
            registry, f"{self.__class__.__module__}.{self.__class__.__name__}"
        )
        await self.entrypoint.connect(bus)


class ServiceGroup(UserDict):

    data: Dict[str, Service]

    def apply(self, method_name: str, order: Optional[List[str]] = None):

        if order:
            for s_name in order:
                if s_name in self.data:
                    s = self.data[s_name]
                    func = getattr(s, method_name)
                    func()
        else:
            for s in self.data.values():
                func = getattr(s, method_name)
                func()

    async def async_apply(
        self, method_name: str, order: Optional[List[str]] = None
    ) -> List[Any]:

        outputs: List[Any] = []
        if order:
            for s_name in order:
                if s_name in self.data:
                    s = self.data[s_name]
                    func = getattr(s, method_name)
                    outputs.append(await func())
        else:
            for s in self.data.values():
                func = getattr(s, method_name)
                outputs.append(await func())

        return outputs
