from typing import Any, Optional

from aiodistbus import EntryPoint, EventBus, registry

from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy-engine")


class Service:
    def __init__(self, name: str):
        self.name = name
        self.entrypoint = EntryPoint()

    async def __debug(self, data: Optional[Any] = None):
        logger.debug(f"DEBUG {self}: {data}")

    def __str__(self):
        return f"<{self.__class__.__name__}, name={self.name}>"

    async def attach(self, bus: EventBus, debug: bool = False):
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

        if debug:
            await self.entrypoint.on("*", self.__debug)
