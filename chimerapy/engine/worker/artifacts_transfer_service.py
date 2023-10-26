import logging
import pathlib
from typing import Dict, Optional, Tuple

import zmq.asyncio

import chimerapy.engine.config as cpe_config
from chimerapy.engine._logger import fork, getLogger
from chimerapy.engine.networking.zmq_file_transfer_server import ZMQFileServer
from chimerapy.engine.utils import get_ip_address

from ..eventbus import Event, EventBus, TypedObserver
from ..service import Service
from ..states import WorkerState


class ArtifactsTransferService(Service):
    def __init__(
        self,
        name: str,
        event_bus: EventBus,
        state: WorkerState,
        parent_logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name=name)
        self.event_bus = event_bus
        self.observers: Dict[str, TypedObserver] = {}
        self.server: Optional[ZMQFileServer] = None
        self.state = state
        self.addr: Optional[Tuple] = None

        if parent_logger is None:
            parent_logger = getLogger("chimerapy-engine")

        self.logger = fork(parent_logger, self.__class__.__name__)

    async def async_init(self):
        self.observers = {
            "initiate_transfer": TypedObserver(
                "initiate_transfer",
                on_asend=self._initiate_artifacts_transfer,
                handle_event="pass",
            ),
            "connected": TypedObserver(
                "connected", on_asend=self._on_connect, handle_event="unpack"
            ),
            "disconnected": TypedObserver(
                "disconnected", on_asend=self._on_disconnect, handle_event="drop"
            ),
            "shutdown": TypedObserver(
                "shutdown", on_asend=self.shutdown, handle_event="drop"
            ),
        }

        for name, observer in self.observers.items():  # noqa: B007
            await self.event_bus.asubscribe(observer)

    async def _on_connect(self, manager_host: str, manager_port: int) -> None:
        self.addr = (manager_host, manager_port)

    async def _on_disconnect(self) -> None:
        self.addr = None

    def _is_remote_worker(self) -> bool:
        return self.addr is not None and self.addr[0] != get_ip_address()

    async def _initiate_artifacts_transfer(self, event: Event) -> None:
        artifacts_data = event.data
        assert artifacts_data is not None
        if not self._is_remote_worker():
            self.logger.info("Initiating local artifacts transfer")
            await self.event_bus.asend(
                Event(
                    "artifacts_transfer_ready",
                    data={
                        "port": None,
                        "ip": None,
                        "data": artifacts_data,
                        "method": "file_copy",
                    },
                )
            )
            return

        files = {}
        for node_id, data in artifacts_data.items():
            for artifact in data:
                files[f"{node_id}-{artifact['name']}"] = pathlib.Path(artifact["path"])

        self.server = ZMQFileServer(
            context=zmq.asyncio.Context(),
            paths=files,
            credit=cpe_config.get("file-transfer.max-chunks"),
            host=get_ip_address(),
            port=0,
            parent_logger=self.logger,
        )

        await self.server.async_init()
        self.logger.info("Initiating remote artifacts transfer")
        await self.event_bus.asend(
            Event(
                "artifacts_transfer_ready",
                data={
                    "port": self.server.port,
                    "ip": self.server.host,
                    "data": artifacts_data,
                    "method": "zmq",
                },
            )
        )

    async def shutdown(self) -> None:
        await self.server.ashutdown() if self.server else None
