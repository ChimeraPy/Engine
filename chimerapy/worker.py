from typing import Union, Dict, Any, Coroutine, Optional, List
import os
import time
import tempfile
import pathlib
import shutil
import sys
import pickle
import uuid
import collections
import asyncio
import traceback
import multiprocessing as mp
from concurrent.futures import Future

# Third-party Imports
import dill
import aiohttp
from aiohttp import web

from chimerapy import config
from .states import WorkerState, NodeState
from .utils import get_ip_address, waiting_for, async_waiting_for
from .networking import Server, Client, DataChunk
from .networking.enums import (
    NODE_MESSAGE,
    WORKER_MESSAGE,
)
from .node.worker_service import WorkerService
from . import _logger


class Worker:
    def __init__(
        self,
        name: str,
        port: int = 10000,
        delete_temp: bool = True,
        id: Optional[str] = None,
    ):
        """Create a local Worker.

        To execute ``Nodes`` within the main computer that is also housing
        the ``Manager``, it will require a ``Worker`` as well. Therefore,
        it is common to create a ``Worker`` and a ``Manager`` within the
        same computer.

        To create a worker in another machine, you will have to use the
        following command (in the other machine's terminal):

        >>> cp-worker --ip <manager's IP> --port <manager's port> --name <name>

        Args:
            name (str): The name for the ``Worker`` that will be used \
                as reference.
            port (int): The port of the Worker's HTTP server. Select 0 \
                for a random port, mostly when running multiple Worker \
                instances in the same computer.
            delete_temp (bool): After session is over, should the Worker
                delete any of the temporary files.

        """
        # Saving parameters
        if isinstance(id, str):
            id = id
        else:
            id = str(uuid.uuid4())

        # Creating state
        self.state = WorkerState(id=id, name=name, port=port)
        self.nodes_extra = collections.defaultdict(dict)

        # Creating a container for task futures
        self.task_futures: List[Future] = []

        # Instance variables
        self.has_shutdown: bool = False
        self.manager_ack: bool = False
        self.connected_to_manager: bool = False
        self.manager_host = "0.0.0.0"
        self.manager_url = ""

        # Create temporary data folder
        self.delete_temp = delete_temp
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())

        parent_logger = _logger.getLogger("chimerapy-worker")
        self.logger = _logger.fork(parent_logger, name, id)

        # Create server
        self.server = Server(
            port=self.state.port,
            id=self.state.id,
            routes=[
                web.post("/nodes/create", self._async_create_node_route),
                web.post("/nodes/destroy", self._async_destroy_node_route),
                web.get("/nodes/server_data", self._async_report_node_server_data),
                web.post("/nodes/server_data", self._async_process_node_server_data),
                web.get("/nodes/gather", self._async_report_node_gather),
                web.post("/nodes/save", self._async_report_node_saving),
                web.post("/nodes/collect", self._async_send_archive),
                web.post("/nodes/step", self._async_step_route),
                web.post("/packages/load", self._async_load_sent_packages),
                web.post("/nodes/start", self._async_start_nodes_route),
                web.post("/nodes/stop", self._async_stop_nodes_route),
                web.post("/shutdown", self._async_shutdown_route),
            ],
            ws_handlers={
                NODE_MESSAGE.STATUS: self._async_node_status_update,
                NODE_MESSAGE.REPORT_GATHER: self._async_node_report_gather,
            },
            parent_logger=self.logger,
        )

        # Start the server and get the new port address (random if port=0)
        self.server.serve()
        self.state.ip, self.state.port = self.server.host, self.server.port

        self.logger.info(
            f"Worker {self.state.id} running HTTP server at {self.state.ip}:{self.state.port}"
        )

        # Create a log listener to read Node's information
        self.logreceiver = self._start_log_receiver()
        self.logger.debug(f"Log receiver started at port {self.logreceiver.port}")

    def __repr__(self):
        return f"<Worker name={self.state.name} id={self.state.id}>"

    def __str__(self):
        return self.__repr__()

    ####################################################################
    ## Properties
    ####################################################################

    @property
    def id(self) -> str:
        return self.state.id

    @property
    def name(self) -> str:
        return self.state.name

    @property
    def nodes(self) -> Dict[str, NodeState]:
        return self.state.nodes

    @property
    def ip(self) -> str:
        return self.state.ip

    @property
    def port(self) -> int:
        return self.state.port

    ####################################################################
    ## HTTP Routes
    ####################################################################

    async def _async_load_sent_packages(self, request: web.Request):
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
                self.logger.debug(
                    f"{self}: Waiting for package {sent_package}: SUCCESS"
                )
            else:
                self.logger.error(f"{self}: Waiting for package {sent_package}: FAILED")
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
                == True,
                timeout=config.get("worker.timeout.package-delivery"),
            )

            if success:
                self.logger.debug(f"{self}: Package {sent_package} loading: SUCCESS")
            else:
                self.logger.debug(f"{self}: Package {sent_package} loading: FAILED")

            assert (
                package_zip_path.exists()
            ), f"{self}: {package_zip_path} doesn't exists!?"
            sys.path.insert(0, str(package_zip_path))

        # Send message back to the Manager letting them know that
        self.logger.info(f"{self}: Completed loading packages sent by Manager")
        return web.HTTPOk()

    async def _async_create_node_route(
        self,
        request: Optional[web.Request] = None,
        node_config: Optional[Dict[str, Any]] = None,
    ):
        if isinstance(request, web.Request):
            msg_bytes = await request.read()
            msg = pickle.loads(msg_bytes)
        elif isinstance(node_config, dict):
            msg = node_config
        else:
            raise RuntimeError("Invalid node creation, need request or msg")

        node_id = msg["id"]
        success = await self.async_create_node(node_id, msg)

        # Update the manager with the most up-to-date status of the nodes
        response = {
            "success": success,
            "node_state": self.state.nodes[node_id].to_dict(),
        }

        return web.json_response(response)

    async def _async_destroy_node_route(self, request: web.Request):
        msg = await request.json()
        node_id = msg["id"]

        success = await self.async_destroy_node(node_id)

        return web.json_response(
            {"success": success, "worker_state": self.state.to_dict()}
        )

    async def _async_report_node_server_data(self, request: web.Request):

        node_server_data = self._create_node_server_data()
        return web.json_response(
            {"success": True, "node_server_data": node_server_data}
        )

    async def _async_process_node_server_data(self, request: web.Request):
        msg = await request.json()

        self.logger.debug(f"{self}: processing node server data: {msg}")

        await self.server.async_broadcast(
            signal=WORKER_MESSAGE.BROADCAST_NODE_SERVER_DATA,
            data=msg,
        )

        # Now wait until all nodes have responded as CONNECTED
        success = []
        for node_id in self.state.nodes:
            for i in range(config.get("worker.allowed-failures")):
                if await async_waiting_for(
                    condition=lambda: self.state.nodes[node_id].fsm == "CONNECTED",
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    self.logger.debug(f"{self}: Nodes {node_id} has connected: PASS")
                    success.append(True)
                    break
                else:
                    self.logger.debug(f"{self}: Node {node_id} has connected: FAIL")
                    success.append(False)

        if not all(success):
            self.logger.error(f"{self}: Nodes failed to establish P2P connections")

        # After all nodes have been connected, inform the Manager
        self.logger.debug(f"{self}: Informing Manager of processing completion")

        return web.json_response(
            {"success": True, "worker_state": self.state.to_dict()}
        )

    async def _async_step_route(self, request: web.Request):

        if await self.async_step():
            return web.HTTPOk()
        else:
            return web.HTTPError()

    async def _async_start_nodes_route(self, request: web.Request):

        if await self.async_start_nodes():
            return web.HTTPOk()
        else:
            return web.HTTPError()

    async def _async_stop_nodes_route(self, request: web.Request):

        if await self.async_stop_nodes():
            return web.HTTPOk()
        else:
            return web.HTTPError()

    async def _async_shutdown_route(self, request: web.Request):
        # Execute shutdown after returning HTTPOk (prevent Manager stuck waiting)
        task = asyncio.create_task(self.async_shutdown())

        return web.HTTPOk()

    async def _async_report_node_saving(self, request: web.Request):

        # Request saving from Worker to Nodes
        await self.server.async_broadcast(signal=WORKER_MESSAGE.REQUEST_SAVING, data={})

        # Now wait until all nodes have responded as CONNECTED
        success = []
        for i in range(config.get("worker.allowed-failures")):
            for node_id in self.nodes:

                if await async_waiting_for(
                    condition=lambda: self.state.nodes[node_id].fsm == "SAVED",
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    self.logger.debug(
                        f"{self}: Node {node_id} responded to saving request: PASS"
                    )
                    success.append(True)
                    break
                else:
                    self.logger.debug(
                        f"{self}: Node {node_id} responded to saving request: FAIL"
                    )
                    success.append(False)

        if not all(success):
            self.logger.error(f"{self}: Nodes failed to report to saving")

        # Send it back to the Manager
        return web.HTTPOk()

    async def _async_report_node_gather(self, request: web.Request):

        self.logger.debug(f"{self}: reporting to Manager gather request")

        for node_id in self.state.nodes:
            self.nodes_extra[node_id]["response"] = False

        # Request gather from Worker to Nodes
        await self.server.async_broadcast(signal=WORKER_MESSAGE.REQUEST_GATHER, data={})

        # Wait until all Nodes have gather
        success = []
        for node_id in self.state.nodes:
            for i in range(config.get("worker.allowed-failures")):

                if await async_waiting_for(
                    condition=lambda: self.nodes_extra[node_id]["response"] == True,
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    self.logger.debug(
                        f"{self}: Node {node_id} responded to gather: PASS"
                    )
                    success.append(True)
                    break
                else:
                    self.logger.debug(
                        f"{self}: Node {node_id} responded to gather: FAIL"
                    )
                    success.append(False)

                if not all(success):
                    self.logger.error(f"{self}: Nodes failed to report to gather")

        # Gather the data from the nodes!
        gather_data = {"id": self.state.id, "node_data": {}}
        for node_id, node_data in self.nodes_extra.items():
            if node_data["gather"] == None:
                data_chunk = DataChunk()
                data_chunk.add("default", None)
                node_data["gather"] = data_chunk
            gather_data["node_data"][node_id] = node_data["gather"]

        return web.Response(body=pickle.dumps(gather_data))

    async def _async_send_archive(self, request: web.Request):
        msg = await request.json()

        # Default value of success
        success = False

        # If located in the same computer, just move the data
        if self.manager_host == get_ip_address():
            await self._async_send_archive_locally(pathlib.Path(msg["path"]))

        else:
            await self._async_send_archive_remotely(
                self.manager_host, self.manager_port
            )

        # After completion, let the Manager know
        return web.json_response({"id": self.id, "success": success})

    ####################################################################
    ## WS Routes
    ####################################################################

    async def _async_node_report_gather(self, msg: Dict, ws: web.WebSocketResponse):

        # Saving gathering value
        node_state = NodeState.from_dict(msg["data"]["state"])
        node_id = node_state.id
        self.state.nodes[node_id] = node_state

        self.nodes_extra[node_id]["gather"] = msg["data"]["latest_value"]
        self.nodes_extra[node_id]["response"] = True

    async def _async_node_status_update(self, msg: Dict, ws: web.WebSocketResponse):

        self.logger.debug(f"{self}: note_status_update: ", msg)
        node_state = NodeState.from_dict(msg["data"])
        node_id = node_state.id

        # Update our records by grabbing all data from the msg
        self.state.nodes[node_id] = node_state

        # Update Manager on the new nodes status
        if self.connected_to_manager:
            async with aiohttp.ClientSession(self.manager_url) as session:
                async with session.post(
                    "/workers/node_status", data=self.state.to_json()
                ):
                    pass

    ####################################################################
    ## Helper Methods
    ####################################################################

    async def _async_send_archive_locally(self, path: pathlib.Path) -> bool:
        self.logger.debug(f"{self}: sending archive locally")

        # First rename and then move
        delay = 1
        miss_counter = 0
        timeout = 10
        while True:
            try:
                shutil.move(self.tempfolder, path)
                break
            except shutil.Error:  # File already exists!
                break
            except:
                self.logger.error(
                    f"{self}: failed to move tempfolder: {self.tempfolder} to dst: {path}"
                )
                await asyncio.sleep(delay)
                miss_counter += 1
                if miss_counter * delay > timeout:
                    raise TimeoutError("Nodes haven't fully finishing saving!")

        old_folder_name = path / self.tempfolder.name
        new_folder_name = path / f"{self.name}-{self.id}"
        os.rename(old_folder_name, new_folder_name)
        return True

    async def _async_send_archive_remotely(self, host: str, port: int) -> bool:

        self.logger.debug(f"{self}: sending archive via network")

        # Else, send the archive data to the manager via network
        try:
            # Create a temporary HTTP client
            client = Client(self.id, host=host, port=port)
            await client._send_folder_async(self.name, self.tempfolder)
            return True
        except (TimeoutError, SystemError) as error:
            self.delete_temp = False
            self.logger.exception(
                f"{self}: Failed to transmit files to Manager - {error}."
            )

        return False

    def _create_node_server_data(self):

        # Construct simple data structure for Node to address information
        node_server_data = {"id": self.state.id, "nodes": {}}
        for node_id, node_state in self.state.nodes.items():
            node_server_data["nodes"][node_id] = {
                "host": self.state.ip,
                "port": node_state.port,
            }

        return node_server_data

    def _exec_coro(self, coro: Coroutine) -> Future:
        # Submitting the coroutine
        future = self.server._thread.exec(coro)

        # Saving the future for later use
        self.task_futures.append(future)

        return future

    @staticmethod
    def _start_log_receiver() -> "ZMQNodeIDListener":
        log_receiver = _logger.get_node_id_zmq_listener()
        log_receiver.start(register_exit_handlers=True)
        return log_receiver

    ####################################################################
    ## Worker ASync Lifecycle API
    ####################################################################

    async def async_connect(
        self, host: str, port: int, timeout: Union[int, float] = 10.0
    ) -> bool:
        """Connect ``Worker`` to ``Manager``.

        This establish server-client connections between ``Worker`` and
        ``Manager``. To ensure that the connections are close correctly,
        either the ``Manager`` or ``Worker`` should shutdown before
        stopping your program to avoid processes and threads that do
        not shutdown.

        Args:
            host (str): The ``Manager``'s IP address.
            port (int): The ``Manager``'s port number
            timeout (Union[int, float]): Set timeout for the connection.

        Returns:
            Future[bool]: Success in connecting to the Manager

        """

        # Send the request to each worker
        async with aiohttp.ClientSession() as client:
            async with client.post(
                f"http://{host}:{port}/workers/register",
                data=self.state.to_json(),
                timeout=config.get("worker.timeout.info-request"),
            ) as resp:

                if resp.ok:

                    # Get JSON
                    data = await resp.json()

                    config.update_defaults(data.get("config", {}))
                    logs_push_info = data.get("logs_push_info", {})

                    if logs_push_info["enabled"]:
                        self.logger.info(f"{self}: enabling logs push to Manager")
                        for logging_entity in [self.logger, self.logreceiver]:
                            handler = _logger.add_zmq_push_handler(
                                logging_entity,
                                logs_push_info["host"],
                                logs_push_info["port"],
                            )
                            if logging_entity is not self.logger:
                                _logger.add_identifier_filter(handler, self.state.id)

                    # Tracking the state and location of the manager
                    self.connected_to_manager = True
                    self.manager_host = host
                    self.manager_port = port
                    self.manager_url = f"http://{host}:{port}"

                    self.logger.info(
                        f"{self}: connection successful to Manager located at {host}:{port}."
                    )
                    return True

        return False

    async def async_deregister(self) -> bool:

        # Send the request to each worker
        async with aiohttp.ClientSession() as client:
            async with client.post(
                self.manager_url + "/workers/deregister",
                data=self.state.to_json(),
                timeout=config.get("worker.timeout.info-request"),
            ) as resp:

                return resp.ok

        return False

    async def async_create_node(self, node_id: str, msg: Dict) -> bool:

        # Saving name to track it for now
        self.logger.debug(f"{self}: received request for Node {id} creation: {msg}")

        # Saving the node data
        self.state.nodes[node_id] = NodeState(id=node_id)
        self.nodes_extra[node_id]["response"] = False
        self.nodes_extra[node_id]["gather"] = DataChunk()
        self.nodes_extra[node_id].update({k: v for k, v in msg.items() if k != "id"})
        self.logger.debug(f"{self}: created state for <Node {node_id}>")

        # Keep trying to start a process until success
        success = False
        for i in range(config.get("worker.allowed-failures")):

            # Decode the node object
            self.nodes_extra[node_id]["node_object"] = dill.loads(
                self.nodes_extra[node_id]["pickled"]
            )
            self.logger.debug(f"{self}: unpickled <Node {node_id}>")

            # Record the node name
            self.state.nodes[node_id].name = self.nodes_extra[node_id][
                "node_object"
            ].name

            # Create worker service and inject to the Node
            worker_service = WorkerService(
                "worker",
                self.state.ip,
                self.state.port,
                self.tempfolder,
                self.nodes_extra[node_id]["in_bound"],
                self.nodes_extra[node_id]["in_bound_by_name"],
                self.nodes_extra[node_id]["out_bound"],
                self.nodes_extra[node_id]["follow"],
                logging_level=self.logger.level,
                worker_logging_port=self.logreceiver.port,
            )
            worker_service.inject(self.nodes_extra[node_id]["node_object"])
            self.logger.debug(
                f"{self}: injected {self.nodes_extra[node_id]['node_object']} with WorkerService"
            )

            # Create a process to run the Node
            process = mp.Process(target=self.nodes_extra[node_id]["node_object"].run)
            self.nodes_extra[node_id]["process"] = process

            # Start the node
            process.start()
            self.logger.debug(f"{self}: started <Node {node_id}>")

            # Wait until response from node
            success = await async_waiting_for(
                condition=lambda: self.state.nodes[node_id].fsm
                in ["INITIALIZED", "CONNECTED", "READY"],
                timeout=config.get("worker.timeout.node-creation"),
            )

            if success:
                self.logger.debug(f"{self}: {node_id} responding, SUCCESS")
            else:
                # Handle failure
                self.logger.debug(f"{self}: {node_id} responding, FAILED, retry")
                self.nodes_extra[node_id]["node_object"].shutdown()
                self.nodes_extra[node_id]["process"].join()
                self.nodes_extra[node_id]["process"].terminate()
                continue

            # Now we wait until the node has fully initialized and ready-up
            success = await async_waiting_for(
                condition=lambda: self.state.nodes[node_id].fsm
                in ["CONNECTED", "READY"],
                timeout=config.get("worker.timeout.info-request"),
            )

            if success:
                self.logger.debug(f"{self}: {node_id} fully ready, SUCCESS")
                break
            else:
                # Handle failure
                self.logger.debug(f"{self}: {node_id} fully ready, FAILED, retry")
                self.nodes_extra[node_id]["node_object"].shutdown()
                self.nodes_extra[node_id]["process"].join()
                self.nodes_extra[node_id]["process"].terminate()

        if not success:
            self.logger.error(f"{self}: Node {node_id} failed to create")
        else:
            # Mark success
            self.logger.debug(f"{self}: completed node creation: {node_id}")

        return success

    async def async_destroy_node(self, node_id: str) -> bool:

        self.logger.debug(f"{self}: received request for Node {node_id} destruction")
        success = False

        if node_id in self.nodes_extra:
            self.nodes_extra[node_id]["node_object"].shutdown()
            self.nodes_extra[node_id]["process"].join(
                timeout=config.get("worker.timeout.node-shutdown")
            )

            # If that doesn't work, terminate
            if self.nodes_extra[node_id]["process"].exitcode != 0:
                self.logger.warning(f"{self}: Node {node_id} forced shutdown")
                self.nodes_extra[node_id]["process"].terminate()

            if node_id in self.state.nodes:
                del self.state.nodes[node_id]

            success = True

        return success

    async def async_start_nodes(self) -> bool:
        # Send message to nodes to start
        return await self.server.async_broadcast(
            signal=WORKER_MESSAGE.START_NODES, data={}
        )

    async def async_step(self) -> bool:
        # Worker tell all nodes to take a step
        return await self.server.async_broadcast(
            signal=WORKER_MESSAGE.REQUEST_STEP, data={}
        )

    async def async_stop_nodes(self) -> bool:
        # Send message to nodes to start
        return await self.server.async_broadcast(
            signal=WORKER_MESSAGE.STOP_NODES, data={}
        )

    async def async_shutdown(self) -> bool:

        # Check if shutdown has been called already
        if self.has_shutdown:
            self.logger.debug(f"{self}: requested to shutdown when already shutdown.")
            return True
        else:
            self.has_shutdown = True

        self.logger.debug(f"{self}: shutting down!")

        # Sleeping to ensure that other coroutines were executed before
        await asyncio.sleep(0.5)

        # Sending message to Manager that client is shutting down (only
        # if the manager hasn't already set the client to not running)
        success = False
        if self.connected_to_manager:
            try:
                success = await self.async_deregister()
            except:
                self.logger.warning(f"{self}: Failed to properly deregister")

        # Shutdown the Worker 2 Node server
        success = await self.server.async_shutdown()

        # Shutdown nodes from the client (start all shutdown)
        for node_id in self.nodes_extra:
            self.nodes_extra[node_id]["node_object"].shutdown()
            self.nodes_extra[node_id]["process"].join()

        # Then wait until close, or force
        for node_id in self.nodes_extra:
            self.nodes_extra[node_id]["process"].join(
                timeout=config.get("worker.timeout.node-shutdown")
            )

            # If that doesn't work, terminate
            if self.nodes_extra[node_id]["process"].exitcode != 0:
                self.logger.warning(f"{self}: Node {node_id} forced shutdown")
                self.nodes_extra[node_id]["process"].terminate()

            self.logger.debug(f"{self}: Nodes have joined")

        # Delete temp folder if requested
        if self.tempfolder.exists() and self.delete_temp:
            shutil.rmtree(self.tempfolder)

        return success

    ####################################################################
    ## Worker Sync Lifecycle API
    ####################################################################

    def connect(
        self,
        host: str,
        port: int,
        timeout: Union[int, float] = 10.0,
        blocking: bool = True,
    ) -> Union[bool, Future[bool]]:
        """Connect ``Worker`` to ``Manager``.

        This establish server-client connections between ``Worker`` and
        ``Manager``. To ensure that the connections are close correctly,
        either the ``Manager`` or ``Worker`` should shutdown before
        stopping your program to avoid processes and threads that do
        not shutdown.

        Args:
            host (str): The ``Manager``'s IP address.
            port (int): The ``Manager``'s port number
            timeout (Union[int, float]): Set timeout for the connection.

        Returns:
            Future[bool]: Success in connecting to the Manager

        """
        future = self._exec_coro(self.async_connect(host, port, timeout))

        if blocking:
            return future.result(timeout=timeout)
        return future

    def deregister(self) -> Future[bool]:
        return self._exec_coro(self.async_deregister())

    def create_node(self, msg: Dict[str, Any]) -> Future[bool]:
        return self._exec_coro(self.async_create_node(msg["id"], msg))

    def step(self) -> Future[bool]:
        return self._exec_coro(self.async_step())

    def start_nodes(self) -> Future[bool]:
        return self._exec_coro(self.async_start_nodes())

    def stop_nodes(self) -> Future[bool]:
        return self._exec_coro(self.async_stop_nodes())

    def idle(self):

        self.logger.debug(f"{self}: Idle")

        while not self.has_shutdown:
            time.sleep(2)

    def shutdown(self, blocking: bool = True) -> Union[Future[bool], bool]:
        """Shutdown ``Worker`` safely.

        The ``Worker`` needs to shutdown its server, client and ``Nodes``
        in a safe manner, such as setting flag variables and clearing
        out queues.

        Args:
            msg (Dict): Leave empty, required to work when ``Manager`` sends\
            shutdown message to ``Worker``.

        """
        future = self._exec_coro(self.async_shutdown())
        if blocking:
            return future.result(timeout=config.get("manager.timeout.worker-shutdown"))
        return future

    def __del__(self):

        # Also good to shutdown anything that isn't
        if not self.has_shutdown:
            self.shutdown()
