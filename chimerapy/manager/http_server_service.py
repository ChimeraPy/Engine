from typing import List
from concurrent.futures import Future
import traceback

from aiohttp import web

from chimerapy import config
from .manager_service import ManagerService
from ..states import WorkerState
from ..networking.async_loop_thread import AsyncLoopThread
from ..networking import Server
from ..networking.enums import MANAGER_MESSAGE
from .. import _logger

logger = _logger.getLogger("chimerapy")


class HttpServerService(ManagerService):

    ip: str
    port: int

    def __init__(self, name: str, port: int, enable_api: bool, thread: AsyncLoopThread):
        super().__init__(name=name)

        # Save input parameters
        self.name = name
        self.ip = "172.0.0.1"
        self.port = port
        self._enable_api = enable_api
        self._thread = thread

        # Future Container
        self._futures: List[Future] = []

    def start(self):

        # Create server
        self._server = Server(
            port=self.port,
            id="Manager",
            routes=[
                # Worker API
                web.post("/workers/register", self._register_worker_route),
                web.post("/workers/deregister", self._deregister_worker_route),
                web.post("/workers/node_status", self._update_nodes_status),
            ],
        )

        # Runn the Server
        self._server.serve(thread=self._thread)
        logger.info(f"Manager started at {self._server.host}:{self._server.port}")

        # Update the ip and port
        self.ip, self.port = self._server.host, self._server.port
        self.state.ip = self.ip
        self.state.port = self.port

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

    #####################################################################################
    ## Worker -> Manager Routes
    #####################################################################################

    async def _register_worker_route(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        # Register worker
        success = self.services.worker_handler._register_worker(worker_state)

        if success:
            logs_collection_info = self.services.distributed_logging.get_log_info()

            response = {
                "logs_push_info": logs_collection_info,
                "config": config.config,
            }

            await self._broadcast_network_status_update()
            return web.json_response(response)

        else:
            return web.HTTPError()

    async def _deregister_worker_route(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)
        success = self.services.worker_handler._deregister_worker(worker_state.id)

        if success:
            await self._broadcast_network_status_update()
            return web.HTTPOk()
        else:
            return web.HTTPBadRequest()

    async def _update_nodes_status(self, request: web.Request):
        msg = await request.json()
        worker_state = WorkerState.from_dict(msg)

        # Updating nodes status
        self.state.workers[worker_state.id] = worker_state
        logger.debug(f"{self}: Nodes status update to: {self.state.workers}")

        # Relay information to front-end
        await self._broadcast_network_status_update()

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
