import asyncio
import json
import logging
import pathlib
import pickle
from typing import Dict, List

from aiodistbus import EventBus, registry
from aiohttp import web

from ..data_protocols import (
    GatherData,
    NodeDiagnostics,
    NodePubEntry,
    NodePubTable,
    RegisteredMethodData,
    ResultsData,
    ServerMessage,
)
from ..networking import DataChunk, Server
from ..networking.enums import NODE_MESSAGE
from ..node import NodeConfig
from ..service import Service
from ..states import NodeState, WorkerState
from ..utils import update_dataclass


class HttpServerService(Service):
    def __init__(
        self,
        name: str,
        state: WorkerState,
        logger: logging.Logger,
    ):
        super().__init__(name=name)

        # Save input parameters
        self.name = name
        self.state = state
        self.logger = logger

        # Containers
        self.tasks: List[asyncio.Task] = []

        # Create server
        self.server = Server(
            port=self.state.port,
            id=self.state.id,
            routes=[
                web.post("/nodes/create", self._async_create_node_route),
                web.post("/nodes/destroy", self._async_destroy_node_route),
                web.get("/nodes/pub_table", self._async_get_node_pub_table),
                web.post("/nodes/pub_table", self._async_process_node_pub_table),
                web.get("/nodes/gather", self._async_report_node_gather),
                web.post("/nodes/collect", self._async_collect),
                web.post("/nodes/step", self._async_step_route),
                web.post("/nodes/start", self._async_start_nodes_route),
                web.post("/nodes/record", self._async_record_route),
                web.post("/nodes/registered_methods", self._async_request_method_route),
                web.post("/nodes/stop", self._async_stop_nodes_route),
                web.post("/nodes/diagnostics", self._async_diagnostics_route),
                # web.post("/packages/load", self._async_load_sent_packages),
                web.post("/shutdown", self._async_shutdown_route),
            ],
            ws_handlers={
                NODE_MESSAGE.STATUS: self._async_node_status_update,
                NODE_MESSAGE.REPORT_GATHER: self._async_node_report_gather,
                NODE_MESSAGE.REPORT_RESULTS: self._async_node_report_results,
                NODE_MESSAGE.DIAGNOSTICS: self._async_node_diagnostics,
            },
            parent_logger=self.logger,
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
        await self.server.async_serve()

        # Update the ip and port
        self._ip, self._port = self.server.host, self.server.port
        self.state.ip = self.ip
        self.state.port = self.port

        # After updatign the information, then run it!
        await self.entrypoint.emit("after_server_startup")

    @registry.on("shutdown", namespace=f"{__name__}.HttpServerService")
    async def shutdown(self) -> bool:
        return await self.server.async_shutdown()

    ####################################################################
    ## Helper Functions
    ####################################################################

    @registry.on("send", ServerMessage, namespace=f"{__name__}.HttpServerService")
    async def _async_send(self, msg: ServerMessage) -> bool:
        if not isinstance(msg.client_id, str):
            self.logger.error(f"{self}: Missing client_id")
            return False
        return await self.server.async_send(
            client_id=msg.client_id, signal=msg.signal, data=msg.data
        )

    @registry.on("broadcast", ServerMessage, namespace=f"{__name__}.HttpServerService")
    async def _async_broadcast(self, msg: ServerMessage) -> bool:
        return await self.server.async_broadcast(signal=msg.signal, data=msg.data)

    def _create_node_pub_table(self) -> NodePubTable:

        # Construct simple data structure for Node to address information
        node_pub_table = NodePubTable()
        for node_id, node_state in self.state.nodes.items():
            node_entry = NodePubEntry(ip=self.state.ip, port=node_state.port)
            node_pub_table.table[node_id] = node_entry

        return node_pub_table

    async def _collect_and_send(self, path: pathlib.Path):

        # Collect data from the Nodes
        await self.entrypoint.emit("collect")

        # After collecting, request to send the archive
        await self.entrypoint.emit("send_archive", path)

    ####################################################################
    ## HTTP Routes
    ####################################################################

    # async def _async_load_sent_packages(self, request: web.Request) -> web.Response:
    #     msg = await request.json()

    #     # For each package, extract it from the client's tempfolder
    #     # and load it to the sys.path
    #     for sent_package in msg["packages"]:

    #         # Wait until the sent package are started
    #         success = await async_waiting_for(
    #             condition=lambda: f"{sent_package}.zip"
    #             in self.server.file_transfer_records["Manager"],
    #             timeout=config.get("worker.timeout.package-delivery"),
    #         )

    #         if success:
    #             self.logger.debug(
    #                 f"{self}: Waiting for package {sent_package}: SUCCESS"
    #             )
    #         else:
    #             self.logger.error(f"{self}: Waiting for "
    #             "package {sent_package}: FAILED")
    #             return web.HTTPError()

    #         # Get the path
    #         package_zip_path = self.server.file_transfer_records["Manager"][
    #             f"{sent_package}.zip"
    #         ]["dst_filepath"]

    #         # Wait until the sent package is complete
    #         success = await async_waiting_for(
    #             condition=lambda: self.server.file_transfer_records["Manager"][
    #                 f"{sent_package}.zip"
    #             ]["complete"]
    #             is True,
    #             timeout=config.get("worker.timeout.package-delivery"),
    #         )

    #         if success:
    #             self.logger.debug(f"{self}: Package {sent_package} loading: SUCCESS")
    #         else:
    #             self.logger.debug(f"{self}: Package {sent_package} loading: FAILED")

    #         assert (
    #             package_zip_path.exists()
    #         ), f"{self}: {package_zip_path} doesn't exists!?"
    #         sys.path.insert(0, str(package_zip_path))

    #     # Send message back to the Manager letting them know that
    #     return web.HTTPOk()

    async def _async_create_node_route(self, request: web.Request) -> web.Response:

        msg_bytes = await request.read()

        # Create node
        node_config: NodeConfig = pickle.loads(msg_bytes)
        await self.entrypoint.emit("create_node", node_config)

        return web.HTTPOk()

    async def _async_destroy_node_route(self, request: web.Request) -> web.Response:

        msg = await request.json()

        # Destroy Node
        node_id: str = msg["id"]
        await self.entrypoint.emit("destroy_node", node_id)

        return web.HTTPOk()

    async def _async_get_node_pub_table(self, request: web.Request) -> web.Response:
        node_pub_table = self._create_node_pub_table()
        return web.json_response(node_pub_table.to_json())

    async def _async_process_node_pub_table(self, request: web.Request) -> web.Response:

        msg = await request.json()
        node_pub_table: NodePubTable = NodePubTable.from_dict(msg)

        # Broadcasting the node server data
        await self.entrypoint.emit("process_node_pub_table", node_pub_table)

        return web.HTTPOk()

    async def _async_step_route(self, request: web.Request) -> web.Response:

        await self.entrypoint.emit("step_nodes")
        return web.HTTPOk()

    async def _async_start_nodes_route(self, request: web.Request) -> web.Response:

        await self.entrypoint.emit("start_nodes")
        return web.HTTPOk()

    async def _async_record_route(self, request: web.Request) -> web.Response:

        await self.entrypoint.emit("record_nodes")
        return web.HTTPOk()

    async def _async_request_method_route(self, request: web.Request) -> web.Response:

        msg = await request.json()

        # Get event information
        reg_method_data = RegisteredMethodData.from_dict(msg)

        # Send it!
        await self.entrypoint.emit("registered_method", reg_method_data)
        return web.HTTPOk()

    async def _async_stop_nodes_route(self, request: web.Request) -> web.Response:

        await self.entrypoint.emit("stop_nodes")
        return web.HTTPOk()

    async def _async_report_node_gather(self, request: web.Request) -> web.Response:

        await self.entrypoint.emit("gather_nodes")

        self.logger.warning(f"{self}: gather doesn't work ATM.")
        gather_data = {"id": self.state.id, "node_data": {}}
        return web.Response(body=pickle.dumps(gather_data))

    async def _async_collect(self, request: web.Request) -> web.Response:
        data = await request.json()
        asyncio.create_task(self._collect_and_send(pathlib.Path(data["path"])))
        return web.HTTPOk()

    async def _async_diagnostics_route(self, request: web.Request) -> web.Response:

        data = await request.json()

        # Determine if enable/disable
        enable: bool = data["enable"]
        await self.entrypoint.emit("diagnostics", enable)
        return web.HTTPOk()

    async def _async_shutdown_route(self, request: web.Request) -> web.Response:

        # Execute shutdown after returning HTTPOk (prevent Manager stuck waiting)
        self.tasks.append(asyncio.create_task(self.entrypoint.emit("shutdown")))
        return web.HTTPOk()

    ####################################################################
    ## WS Routes
    ####################################################################

    async def _async_node_status_update(self, msg: Dict, ws: web.WebSocketResponse):

        # self.logger.debug(f"{self}: node_status_update: :{msg}")
        node_state = NodeState.from_dict(msg["data"])

        # Update our records by grabbing all data from the msg
        if node_state.id in self.state.nodes and node_state:

            # Update the node state
            update_dataclass(self.state.nodes[node_state.id], node_state)
            await self.entrypoint.emit("WorkerState.changed", self.state)

    async def _async_node_report_gather(self, msg: Dict, ws: web.WebSocketResponse):
        gather_data = GatherData.from_dict(msg["data"])
        gather_data.output = DataChunk.from_json(gather_data.output)
        await self.entrypoint.emit("update_gather", gather_data)

    async def _async_node_report_results(self, msg: Dict, ws: web.WebSocketResponse):
        results = ResultsData.from_dict(msg["data"])
        await self.entrypoint.emit("update_results", results)

    async def _async_node_diagnostics(self, msg: Dict, ws: web.WebSocketResponse):

        # self.logger.debug(f"{self}: received diagnostics: {msg}")

        # Create the entry and update the table
        diag = NodeDiagnostics.from_dict(msg["data"])
        if diag.node_id in self.state.nodes:
            self.state.nodes[diag.node_id].diagnostics = diag
