import socket
from datetime import datetime
from typing import Optional, Dict

from zeroconf import ServiceInfo, Zeroconf

from chimerapy.engine import _logger
from ..states import ManagerState
from ..eventbus import EventBus, TypedObserver
from ..service import Service

logger = _logger.getLogger("chimerapy-engine")


class ZeroconfService(Service):

    enabled: bool

    def __init__(self, name: str, eventbus: EventBus, state: ManagerState):
        super().__init__(name=name)

        # Save information
        self.name = name
        self.eventbus = eventbus
        self.state = state

        # Creating zeroconf variables
        self.zeroconf: Optional[Zeroconf] = None
        self.enabled: bool = False

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "after_server_startup": TypedObserver(
                "after_server_startup", on_asend=self.start, handle_event="drop"
            ),
            "shutdown": TypedObserver(
                "shutdown", on_asend=self.shutdown, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    def start(self):

        # Create the zeroconf service name
        self.service_name = f"chimerapy-{self.state.id}._http._tcp.local."

        # Create service information
        self.zeroconf_info = ServiceInfo(
            "_http._tcp.local.",
            self.service_name,
            addresses=[socket.inet_aton(self.state.ip)],
            port=self.state.port,
            properties={
                "path": str(self.state.logdir),
                "timestamp": datetime.now().strftime("%Y_%m_%d_%H_%M_%S"),
            },
        )

    async def shutdown(self):
        await self.disable()

    #####################################################################################
    ## Helper Methods
    #####################################################################################

    async def enable(self) -> bool:

        if not self.enabled:

            # Start Zeroconf Service
            self.zeroconf = Zeroconf()
            await self.zeroconf.async_register_service(self.zeroconf_info, ttl=60)
            logger.info(f"{self}: Started Zeroconf Service named {self.service_name}")

            # Mark the service
            self.enabled = True

        return True

    async def disable(self) -> bool:

        if self.enabled:

            # Unregister the service and close the zeroconf instance
            if self.zeroconf:
                await self.zeroconf.async_unregister_service(self.zeroconf_info)
                self.zeroconf.close()

            # Mark the service
            self.enabled = False

        return True
