import sys
import pickle
import asyncio
import pathlib
import traceback
import enum
from typing import Dict, Optional

from aiohttp import web

from chimerapy.engine import config
from .worker_service import WorkerService
from chimerapy.engine.states import NodeState
from ..networking import Server
from ..networking.async_loop_thread import AsyncLoopThread
from ..networking.enums import NODE_MESSAGE
from chimerapy.engine.utils import async_waiting_for, get_ip_address


class HttpServerService(WorkerService):
    def __init__(self, name: str, thread: Optional[AsyncLoopThread] = None):

        # Save input parameters
        self.name = name
        self.thread = thread

    def start(self):

        # Create server
        self.server = Server(
            port=self.worker.state.port,
            id=self.worker.state.id,
            routes=[
                web.post("/nodes/create", self._async_create_node_route),
                web.post("/nodes/destroy", self._async_destroy_node_route),
                web.get("/nodes/server_data", self._async_report_node_server_data),
                web.post("/nodes/server_data", self._async_process_node_server_data),
                web.get("/nodes/gather", self._async_report_node_gather),
                web.post("/nodes/collect", self._async_send_archive),
                web.post("/nodes/step", self._async_step_route),
                web.post("/packages/load", self._async_load_sent_packages),
                web.post("/nodes/start", self._async_start_nodes_route),
                web.post("/nodes/record", self._async_record_route),
                web.post("/nodes/registered_methods", self._async_request_method_route),
                web.post("/nodes/stop", self._async_stop_nodes_route),
                web.post("/shutdown", self._async_shutdown_route),
            ],
            ws_handlers={
                NODE_MESSAGE.STATUS: self._async_node_status_update,
                NODE_MESSAGE.REPORT_GATHER: self._async_node_report_gather,
                NODE_MESSAGE.REPORT_RESULTS: self._async_node_report_results,
            },
            parent_logger=self.worker.logger,
        )

        # Start the server and get the new port address (random if port=0)
        self.server.serve(thread=self.thread)
        self.worker.state.ip, self.worker.state.port = (
            self.server.host,
            self.server.port,
        )
        self.worker.logger.info(
            f"Worker: {self.worker.state.id} running HTTP server at "
            f"{self.worker.state.ip}:{self.worker.state.port}"
        )

    async def shutdown(self) -> bool:
        return await self.server.async_shutdown()

    ####################################################################
    ## Helper Functions
    ####################################################################

    async def _async_send(self, client_id: str, signal: enum.Enum, data: Dict) -> bool:
        return await self.server.async_send(
            client_id=client_id, signal=signal, data=data
        )

    async def _async_broadcast(self, signal: enum.Enum, data: Dict) -> bool:
        return await self.server.async_broadcast(signal=signal, data=data)

    def _create_node_server_data(self) -> Dict:

        # Construct simple data structure for Node to address information
        node_server_data = {"id": self.worker.state.id, "nodes": {}}
        for node_id, node_state in self.worker.state.nodes.items():
            node_server_data["nodes"][node_id] = {
                "host": self.worker.state.ip,
                "port": node_state.port,
            }

        return node_server_data

    ####################################################################
    ## HTTP Routes
    ####################################################################

    async def _async_load_sent_packages(self, request: web.Request) -> web.Response:
        msg = await request.json()

        # For each package, extract it from the client's tempfolder
        # and load it to the sys.path
        for sent_package in msg["packages"]:

            # Wait until the sent package are started
            success = await async_waiting_for(
                condition=lambda: f"{sent_package}.zip"
                in self.server.file_transfer_records["Manager"],
                timeout=config.get("worker.timeout.package-delivery"),
            )

            if success:
                self.worker.logger.debug(
                    f"{self}: Waiting for package {sent_package}: SUCCESS"
                )
            else:
                self.worker.logger.error(
                    f"{self}: Waiting for package {sent_package}: FAILED"
                )
                return web.HTTPError()

            # Get the path
            package_zip_path = self.server.file_transfer_records["Manager"][
                f"{sent_package}.zip"
            ]["dst_filepath"]

            # Wait until the sent package is complete
            success = await async_waiting_for(
                condition=lambda: self.server.file_transfer_records["Manager"][
                    f"{sent_package}.zip"
                ]["complete"]
                is True,
                timeout=config.get("worker.timeout.package-delivery"),
            )

            if success:
                self.worker.logger.debug(
                    f"{self}: Package {sent_package} loading: SUCCESS"
                )
            else:
                self.worker.logger.debug(
                    f"{self}: Package {sent_package} loading: FAILED"
                )

            assert (
                package_zip_path.exists()
            ), f"{self}: {package_zip_path} doesn't exists!?"
            sys.path.insert(0, str(package_zip_path))

        # Send message back to the Manager letting them know that
        self.worker.logger.info(f"{self}: Completed loading packages sent by Manager")
        return web.HTTPOk()

    async def _async_create_node_route(self, request: web.Request) -> web.Response:
        msg_bytes = await request.read()
        node_config = pickle.loads(msg_bytes)

        success = await self.worker.services.node_handler.async_create_node(node_config)

        # Update the manager with the most up-to-date status of the nodes
        response = {
            "success": success,
            "node_state": self.worker.state.nodes[node_config.id].to_dict(),
        }

        return web.json_response(response)

    async def _async_destroy_node_route(self, request: web.Request) -> web.Response:
        msg = await request.json()
        node_id = msg["id"]

        success = await self.worker.services.node_handler.async_destroy_node(node_id)

        return web.json_response(
            {"success": success, "worker_state": self.worker.state.to_dict()}
        )

    async def _async_report_node_server_data(
        self, request: web.Request
    ) -> web.Response:

        node_server_data = self._create_node_server_data()
        return web.json_response(
            {"success": True, "node_server_data": node_server_data}
        )

    async def _async_process_node_server_data(
        self, request: web.Request
    ) -> web.Response:
        msg = await request.json()

        self.worker.logger.debug(f"{self}: processing node server data: {msg}")

        # Broadcasting the node server data
        success = (
            await self.worker.services.node_handler.async_process_node_server_data(msg)
        )

        # After all nodes have been connected, inform the Manager
        self.worker.logger.debug(f"{self}: Informing Manager of processing completion")

        return web.json_response(
            {"success": success, "worker_state": self.worker.state.to_dict()}
        )

    async def _async_step_route(self, request: web.Request) -> web.Response:

        if await self.worker.services.node_handler.async_step():
            return web.HTTPOk()
        else:
            return web.HTTPError()

    async def _async_start_nodes_route(self, request: web.Request) -> web.Response:

        if await self.worker.services.node_handler.async_start_nodes():
            return web.HTTPOk()
        else:
            return web.HTTPError()

    async def _async_record_route(self, request: web.Request) -> web.Response:

        if await self.worker.services.node_handler.async_record_nodes():
            return web.HTTPOk()
        else:
            return web.HTTPError()

    async def _async_request_method_route(self, request: web.Request) -> web.Response:
        msg = await request.json()

        # Get information
        node_id = msg["node_id"]
        method_name = msg["method_name"]
        params = msg["params"]

        # # Make the request and get results
        results = (
            await self.worker.services.node_handler.async_request_registered_method(
                node_id, method_name, params
            )
        )

        return web.json_response(results)

    async def _async_stop_nodes_route(self, request: web.Request) -> web.Response:

        if await self.worker.services.node_handler.async_stop_nodes():
            return web.HTTPOk()
        else:
            return web.HTTPError()

    async def _async_shutdown_route(self, request: web.Request) -> web.Response:
        # Execute shutdown after returning HTTPOk (prevent Manager stuck waiting)
        self.worker.shutdown_task = asyncio.create_task(self.worker.async_shutdown())

        return web.HTTPOk()

    async def _async_report_node_gather(self, request: web.Request) -> web.Response:

        gather_data = await self.worker.services.node_handler.async_gather()
        return web.Response(body=pickle.dumps(gather_data))

    async def _async_send_archive(self, request: web.Request) -> web.Response:
        msg = await request.json()

        # Collect data from the Nodes
        success = await self.worker.services.node_handler.async_collect()

        # If located in the same computer, just move the data
        if success:
            host, port = self.worker.services.http_client.get_address()
            try:
                if host == get_ip_address():
                    success = (
                        await self.worker.services.http_client._send_archive_locally(
                            pathlib.Path(msg["path"])
                        )
                    )

                else:
                    success = (
                        await self.worker.services.http_client._send_archive_remotely(
                            host, port
                        )
                    )
            except Exception:
                self.worker.logger.error(traceback.format_exc())
                return web.HTTPError()

        # After completion, let the Manager know
        return web.json_response({"id": self.worker.id, "success": success})

    ####################################################################
    ## WS Routes
    ####################################################################

    async def _async_node_status_update(self, msg: Dict, ws: web.WebSocketResponse):

        self.worker.logger.debug(f"{self}: note_status_update: ", msg)
        node_state = NodeState.from_dict(msg["data"])
        node_id = node_state.id

        # Update our records by grabbing all data from the msg
        if node_id in self.worker.state.nodes:
            self.worker.state.nodes[node_id] = node_state

        # Update Manager on the new nodes status
        if self.worker.services.http_client.connected_to_manager:
            await self.worker.services.http_client._async_node_status_update()

    async def _async_node_report_gather(self, msg: Dict, ws: web.WebSocketResponse):

        # Saving gathering value
        node_state = NodeState.from_dict(msg["data"]["state"])
        node_id = node_state.id

        if node_id in self.worker.state.nodes:
            self.worker.state.nodes[node_id] = node_state

        self.worker.services.node_handler.update_gather(
            node_id, msg["data"]["latest_value"]
        )

    async def _async_node_report_results(self, msg: Dict, ws: web.WebSocketResponse):
        self.worker.logger.debug(f"{self}: node report results: {msg}")

        node_id = msg["data"]["node_id"]
        self.worker.services.node_handler.update_results(node_id, msg["data"]["output"])
