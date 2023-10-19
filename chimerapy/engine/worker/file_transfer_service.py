import logging
from typing import List, Dict


from ..service import Service
from ..eventbus import Event, EventBus, TypedObserver

from chimerapy.engine._logger import fork



class FileTransferService(Service):
    def __init__(self, name: str, event_bus: EventBus, logger: logging.Logger):
        super().__init__(name=name)
        self.event_bus = event_bus
        self.observers: Dict[str, TypedObserver] = {}
        self.logger = logger

    async def async_init(self):
        self.observers = {
            "artifacts": TypedObserver(
                "artifacts",
                self._initiate_artifacts_transfer,
                handle_event="pass"
            )
        }

        for name, observer in self.observers.items():
            await self.event_bus.asubscribe(observer)

    async def _initiate_artifacts_transfer(self, event: Event):
        artifacts_data = event.data
        for name, data in artifacts_data.items():
            print(data, name)
