import traceback
from concurrent.futures import Future
from typing import List, Dict

from aiohttp import web

from chimerapy.engine import config
from chimerapy.engine import _logger
from ..eventbus import EventBus, Event, TypedObserver
from ..service import Service
from ..utils import update_dataclass
from ..states import WorkerState, ManagerState
from ..networking.async_loop_thread import AsyncLoopThread
from ..networking import Server
from ..networking.enums import MANAGER_MESSAGE
from .events import (
    MoveTransferredFilesEvent,
    WorkerRegisterEvent,
    WorkerDeregisterEvent,
    UpdateSendArchiveEvent,
)

logger = _logger.getLogger("chimerapy-engine")


class HttpServerService(Service):
    def __init__(
        self,
        name: str,
        port: int,
        enable_api: bool,
        thread: AsyncLoopThread,
        eventbus: EventBus,
        state: ManagerState,
    ):
        super().__init__(name=name)

        # Save input parameters
        self.name = name
        self._ip = "172.0.0.1"
        self._port = port
        self._enable_api = enable_api
        self._thread = thread
        self.eventbus = eventbus
        self.state = state

        # Future Container
        self._futures: List[Future] = []

        # Create server
        self._server = Server(
            port=self.port,
            id="Manager",
            routes=[
                # Worker API
                web.get("/", self._home),
                web.post("/workers/register", self._register_worker_route),
                web.post("/workers/deregister", self._deregister_worker_route),
                web.post("/workers/node_status", self._update_nodes_status),
                web.post("/workers/send_archive", self._update_send_archive),
            ],
            thread=self._thread,
        )

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "start": TypedObserver("start", on_asend=self.start, handle_event="drop"),
            "ManagerState.changed": TypedObserver(
                "ManagerState.changed",
                on_asend=self._broadcast_network_status_update,
                handle_event="drop",
            ),
            "shutdown": TypedObserver(
                "shutdown", on_asend=self.shutdown, handle_event="drop"
            ),
            "move_transferred_files": TypedObserver(
                "move_transferred_files",
                MoveTransferredFilesEvent,
                on_asend=self.move_transferred_files,
                handle_event="unpack",
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    @property
    def ip(self) -> str:
        return self._ip

    @property
    def port(self) -> int:
        return self._port

    @property
    def url(self) -> str:
        return f"http://{self._ip}:{self._port}"

    async def start(self):

        # Runn the Server
        await self._server.async_serve()

        # Update the ip and port
        self._ip, self._port = self._server.host, self._server.port
        self.state.ip = self.ip
        self.state.port = self.port

        # After updatign the information, then run it!
        await self.eventbus.asend(Event("after_server_startup"))

    async def shutdown(self) -> bool:

        # Finish any other tasks
        self._future_flush()

        # Then, shutdown server
        return await self._server.async_shutdown()

    ####################################################################
    ## Helper Functions
    ####################################################################

    def _future_flush(self):

        for future in self._futures:
            try:
                future.result(timeout=config.get("manager.timeout.info-request"))
            except Exception:
                logger.error(traceback.format_exc())

    async def move_transferred_files(self, worker_state: WorkerState) -> bool:
        return await self._server.move_transferred_files(
            self.state.logdir, owner=worker_state.name, owner_id=worker_state.id
        )

    #####################################################################################
    ## Manager User Routes
    #####################################################################################

    async def _home(self, request: web.Request):
        return web.Response(text="ChimeraPy Manager running!")

    #####################################################################################
    ## Worker -> Manager Routes
    #####################################################################################

    async def _register_worker_route(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        # Register worker
        await self.eventbus.asend(
            Event("worker_register", WorkerRegisterEvent(worker_state))
        )

        response = {
            "logs_push_info": {
                "enabled": self.state.log_sink_enabled,
                "host": self.ip,
                "port": self.port,
            },
            "config": config.config,
        }

        # Broadcast changes
        return web.json_response(response)

    async def _deregister_worker_route(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        # Deregister worker
        await self.eventbus.asend(
            Event("worker_deregister", WorkerDeregisterEvent(worker_state))
        )

        return web.HTTPOk()

    async def _update_nodes_status(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        # Updating nodes status
        if worker_state.id in self.state.workers:
            update_dataclass(self.state.workers[worker_state.id], worker_state)
        else:
            logger.warning(f"{self}: non-registered Worker update: {worker_state.id}")

        return web.HTTPOk()

    async def _update_send_archive(self, request: web.Request):
        msg = await request.json()
        event_data = UpdateSendArchiveEvent(**msg)
        await self.eventbus.asend(Event("update_send_archive", event_data))
        return web.HTTPOk()

    #####################################################################################
    ## Front-End API
    #####################################################################################

    async def _broadcast_network_status_update(self):

        if not self._enable_api:
            return

        await self._server.async_broadcast(
            signal=MANAGER_MESSAGE.NETWORK_STATUS_UPDATE,
            data=self.state.to_dict(),
        )
