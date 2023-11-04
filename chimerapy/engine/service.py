from typing import Optional

from aiodistbus import EntryPoint, EventBus, registry


class Service:
    def __init__(self, name: str):
        self.name = name
        self.entrypoint = EntryPoint()

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
        await self.entrypoint.connect(bus)
        await self.entrypoint.use(
            registry,
            b_args=[self],
            namespace=f"{self.__class__.__module__}.{self.__class__.__name__}",
        )
