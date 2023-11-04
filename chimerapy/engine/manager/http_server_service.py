import traceback
from concurrent.futures import Future
from typing import List

from aiodistbus import registry
from aiohttp import web

from chimerapy.engine import _logger, config

from ..data_protocols import UpdateSendArchiveData
from ..networking import Server
from ..networking.enums import MANAGER_MESSAGE
from ..service import Service
from ..states import ManagerState, WorkerState
from ..utils import update_dataclass

logger = _logger.getLogger("chimerapy-engine")


class HttpServerService(Service):
    def __init__(
        self,
        name: str,
        port: int,
        enable_api: bool,
        state: ManagerState,
    ):
        super().__init__(name=name)

        # Save input parameters
        self.name = name
        self._ip = "172.0.0.1"
        self._port = port
        self._enable_api = enable_api
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
        )

    @property
    def ip(self) -> str:
        return self._ip

    @property
    def port(self) -> int:
        return self._port

    @property
    def url(self) -> str:
        return f"http://{self._ip}:{self._port}"

    @registry.on("start", namespace=f"{__name__}.HttpServerService")
    async def start(self):

        # Runn the Server
        await self._server.async_serve()

        # Update the ip and port
        self._ip, self._port = self._server.host, self._server.port
        self.state.ip = self.ip
        self.state.port = self.port

        # After updatign the information, then run it!
        await self.entrypoint.emit("after_server_startup")

    @registry.on("shutdown", namespace=f"{__name__}.HttpServerService")
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

    @registry.on(
        "move_transferred_files", WorkerState, namespace=f"{__name__}.HttpServerService"
    )
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
        await self.entrypoint.emit("worker_register", worker_state)

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
        await self.entrypoint.emit("worker_deregister", worker_state)

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
        event_data = UpdateSendArchiveData(**msg)
        await self.entrypoint.emit("update_send_archive", event_data)
        return web.HTTPOk()

    #####################################################################################
    ## Front-End API
    #####################################################################################

    @registry.on("ManagerState.changed", namespace=f"{__name__}.HttpServerService")
    async def _broadcast_network_status_update(self):

        if not self._enable_api:
            return

        await self._server.async_broadcast(
            signal=MANAGER_MESSAGE.NETWORK_STATUS_UPDATE,
            data=self.state.to_dict(),
        )
