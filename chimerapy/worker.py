from typing import Union, Dict, Any, Coroutine, Optional
import socket
import os
import time
import tempfile
import pathlib
import shutil
import sys
import json
import pickle

# Third-party Imports
import dill
import aiohttp
from aiohttp import web
import requests

from chimerapy import config
from .utils import get_ip_address, waiting_for, async_waiting_for
from .networking import Server, Client, DataChunk
from .networking.enums import (
    MANAGER_MESSAGE,
    NODE_MESSAGE,
    WORKER_MESSAGE,
)
from . import _logger
from .logreceiver import LogReceiver

logger = _logger.getLogger("chimerapy-worker")


class Worker:
    def __init__(self, name: str, port: int = 10000, delete_temp: bool = True):
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
        self.port = port
        self.name = name

        # Instance variables
        self.has_shutdown: bool = False
        self.nodes: Dict = {}
        self.manager_ack: bool = False
        self.connected_to_manager: bool = False
        self.manager_host = "0.0.0.0"

        # Create temporary data folder
        self.delete_temp = delete_temp
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())

        # Create server
        self.server = Server(
            port=self.port,
            name=self.name,
            routes=[
                web.post("/nodes/create", self.async_create_node),
                web.get("/nodes/server_data", self.report_node_server_data),
                web.post("/nodes/server_data", self.process_node_server_data),
                web.get("/nodes/gather", self.report_node_gather),
                web.post("/nodes/save", self.report_node_saving),
                web.post("/nodes/collect", self.send_archive),
                web.post("/nodes/step", self.async_step),
                web.post("/packages/load", self.load_sent_packages),
                web.post("/nodes/start", self.async_start_nodes),
                web.post("/nodes/stop", self.async_stop_nodes),
                web.post("/shutdown", self.async_shutdown),
            ],
            ws_handlers={
                NODE_MESSAGE.STATUS: self.node_status_update,
                NODE_MESSAGE.REPORT_GATHER: self.node_report_gather,
            },
        )

        # Start the server and get the new port address (random if port=0)
        self.server.serve()
        self.host, self.port = self.server.host, self.server.port
        logger.info(
            f"Worker {self.name} running HTTP server at {self.host}:{self.port}"
        )

        # Create a log listener to read Node's information
        self.log_receiver = LogReceiver()
        self.log_receiver.start()

    def __repr__(self):
        return f"<Worker {self.name}>"

    def __str__(self):
        return self.__repr__()

    ####################################################################
    ## Manager -> Worker
    ####################################################################

    async def load_sent_packages(self, request: web.Request):
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
                logger.debug(f"{self}: Waiting for package {sent_package}: SUCCESS")
            else:
                logger.error(f"{self}: Waiting for package {sent_package}: FAILED")
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
                logger.debug(f"{self}: Package {sent_package} loading: SUCCESS")
            else:
                logger.debug(f"{self}: Package {sent_package} loading: FAILED")

            assert (
                package_zip_path.exists()
            ), f"{self}: {package_zip_path} doesn't exists!?"
            sys.path.insert(0, str(package_zip_path))

        # Send message back to the Manager letting them know that
        logger.info(f"{self}: Completed loading packages sent by Manager")
        return web.HTTPOk()

    async def async_create_node(self, request: web.Request):
        msg_bytes = await request.read()
        msg = pickle.loads(msg_bytes)

        # Saving name to track it for now
        node_name = msg["node_name"]
        logger.debug(f"{self}: received request for Node {node_name} creation: {msg}")

        # Saving the node data
        self.nodes[node_name] = {k: v for k, v in msg.items() if k != "node_name"}
        self.nodes[node_name]["status"] = {
            "INIT": 0,
            "CONNECTED": 0,
            "READY": 0,
            "FINISHED": 0,
        }
        self.nodes[node_name]["response"] = False
        self.nodes[node_name]["gather"] = None

        # Keep trying to start a process until success
        success = False
        for i in range(config.get("worker.allowed-failures")):

            # Decode the node object
            self.nodes[node_name]["node_object"] = dill.loads(
                self.nodes[node_name]["pickled"]
            )

            # Provide configuration information to the node once in the client
            self.nodes[node_name]["node_object"].config(
                self.host,
                self.port,
                self.tempfolder,
                self.nodes[node_name]["in_bound"],
                self.nodes[node_name]["out_bound"],
                self.nodes[node_name]["follow"],
                logging_level=logger.level,
                worker_logging_port=self.log_receiver.port,
            )

            # Before starting, over write the pid
            self.nodes[node_name]["node_object"]._parent_pid = os.getpid()

            # Start the node
            self.nodes[node_name]["node_object"].start()
            logger.debug(f"{self}: started <Node {node_name}>")

            # Wait until response from node
            success = await async_waiting_for(
                condition=lambda: self.nodes[node_name]["response"] == True,
                timeout=config.get("worker.timeout.node-creation"),
            )

            if success:
                logger.debug(f"{self}: {node_name} responding, SUCCESS")
            else:
                # Handle failure
                logger.debug(f"{self}: {node_name} responding, FAILED, retry")
                self.nodes[node_name]["node_object"].shutdown()
                self.nodes[node_name]["node_object"].terminate()
                continue

            # Now we wait until the node has fully initialized and ready-up
            success = await async_waiting_for(
                condition=lambda: self.nodes[node_name]["status"]["READY"] == True,
                timeout=config.get("worker.timeout.info-request"),
            )

            if success:
                logger.debug(f"{self}: {node_name} fully ready, SUCCESS")
                break
            else:
                # Handle failure
                logger.debug(f"{self}: {node_name} fully ready, FAILED, retry")
                self.nodes[node_name]["node_object"].shutdown()
                self.nodes[node_name]["node_object"].terminate()

        if not success:
            logger.error(f"{self}: Node {node_name} failed to create")
        else:
            # Mark success
            logger.debug(f"{self}: completed node creation: {self.nodes}")

        # Update the manager with the most up-to-date status of the nodes
        nodes_status = {k: self.nodes[k]["status"] for k in self.nodes}

        return web.json_response({"success": success, "nodes_status": nodes_status})

    async def report_node_server_data(self, request: web.Request):

        node_server_data = self.create_node_server_data()
        return web.json_response(
            {"success": True, "node_server_data": node_server_data}
        )

    async def process_node_server_data(self, request: web.Request):
        msg = await request.json()

        logger.debug(f"{self}: processing node server data")

        await self.server.async_broadcast(
            signal=WORKER_MESSAGE.BROADCAST_NODE_SERVER_DATA,
            data=msg,
        )

        # Now wait until all nodes have responded as CONNECTED
        success = False
        for i in range(config.get("worker.allowed-failures")):
            if await self.wait_until_all_nodes_responded(
                timeout=config.get("worker.timeout.info-request"),
                attribute="CONNECTED",
                status=True,
            ):
                logger.debug(f"{self}: Nodes have been connected.")
                success = True
                break

        if not success:
            logger.error(f"{self}: Nodes failed to establish P2P connections")

        # After all nodes have been connected, inform the Manager
        logger.debug(f"{self}: Informing Manager of processing completion")

        return web.json_response(
            {
                "success": True,
                "nodes_status": {k: self.nodes[k]["status"] for k in self.nodes},
            }
        )

    async def async_step(self, request: web.Request):

        # Worker tell all nodes to take a step
        await self.server.async_broadcast(signal=WORKER_MESSAGE.REQUEST_STEP, data={})

        return web.HTTPOk()

    async def async_start_nodes(self, request: web.Request):

        # Send message to nodes to start
        await self.server.async_broadcast(signal=WORKER_MESSAGE.START_NODES, data={})

        return web.HTTPOk()

    async def async_stop_nodes(self, request: web.Request):

        # Send message to nodes to start
        await self.server.async_broadcast(signal=WORKER_MESSAGE.STOP_NODES, data={})

        return web.HTTPOk()

    async def async_shutdown(self, request: web.Request):
        self.shutdown()

        return web.HTTPOk()

    async def report_node_saving(self, request: web.Request):

        # Marking as all false
        self.mark_all_response_as_false_for_nodes()

        # Now wait until all nodes have responded as CONNECTED
        success = False
        for i in range(config.get("worker.allowed-failures")):

            # Request saving from Worker to Nodes
            await self.server.async_broadcast(
                signal=WORKER_MESSAGE.REQUEST_SAVING, data={}
            )

            if await self.wait_until_all_nodes_responded(
                timeout=config.get("worker.timeout.info-request"),
                status=True,
                attribute="FINISHED",
            ):
                logger.debug(f"{self}: Nodes responded to saving request.")
                success = True
                break

        if not success:
            logger.error(f"{self}: Nodes failed to report to saving")

        # Send it back to the Manager
        return web.HTTPOk()

    async def report_node_gather(self, request: web.Request):

        logger.debug(f"{self}: reporting to Manager gather request")

        # Marking as all false
        self.mark_all_response_as_false_for_nodes()

        # Wait until all Nodes have gather
        success = False
        for i in range(config.get("worker.allowed-failures")):

            # Request gather from Worker to Novdes
            await self.server.async_broadcast(
                signal=WORKER_MESSAGE.REQUEST_GATHER, data={}
            )

            if await self.wait_until_all_nodes_responded(
                timeout=config.get("worker.timeout.info-request")
            ):
                logger.debug(f"{self}: Nodes responded to gather.")
                success = True
                break

        if not success:
            logger.error(f"{self}: Nodes failed to report to gather")

        # Gather the data from the nodes!
        gather_data = {"name": self.name, "node_data": {}}
        for node_name, node_data in self.nodes.items():
            if node_data["gather"] == None:
                data_chunk = DataChunk()
                data_chunk.add("default", None)
                node_data["gather"] = data_chunk
            gather_data["node_data"][node_name] = node_data["gather"]._serialize()

        return web.Response(body=pickle.dumps(gather_data))

    async def send_archive(self, request: web.Request):
        msg = await request.json()

        # Default value of success
        success = False

        # If located in the same computer, just move the data
        if self.manager_host == get_ip_address():

            logger.debug(f"{self}: sending archive locally")

            # First rename and then move
            delay = 1
            miss_counter = 0
            timeout = 10
            while True:
                try:
                    shutil.move(self.tempfolder, pathlib.Path(msg["path"]))
                    break
                except shutil.Error:  # File already exists!
                    break
                except:
                    time.sleep(delay)
                    miss_counter += 1
                    if miss_counter * delay > timeout:
                        raise TimeoutError("Nodes haven't fully finishing saving!")

            old_folder_name = pathlib.Path(msg["path"]) / self.tempfolder.name
            new_folder_name = pathlib.Path(msg["path"]) / self.name
            os.rename(old_folder_name, new_folder_name)

        else:

            logger.debug(f"{self}: sending archive via network")

            # Else, send the archive data to the manager via network
            try:
                # Create a temporary HTTP client
                client = Client(
                    self.name, host=self.manager_host, port=self.manager_port
                )
                # client.send_file(sender_name=self.name, filepath=zip_package_dst)
                await client._send_folder_async(self.name, self.tempfolder)
                success = True
            except (TimeoutError, SystemError) as error:
                self.delete_temp = False
                logger.exception(
                    f"{self}: Failed to transmit files to Manager - {error}."
                )
                success = False

        # After completion, let the Manager know
        return web.json_response({"name": self.name, "success": success})

    ####################################################################
    ## Worker <-> Node
    ####################################################################

    async def node_report_gather(self, msg: Dict, ws: web.WebSocketResponse):

        # Saving name to track it for now
        node_name = msg["data"]["node_name"]
        self.nodes[node_name]["gather"] = msg["data"]["latest_value"]
        self.nodes[node_name]["response"] = True

    async def node_status_update(self, msg: Dict, ws: web.WebSocketResponse):

        # Saving name to track it for now
        node_name = msg["data"]["node_name"]
        status = msg["data"]["status"]

        # Update our records by grabbing all data from the msg
        self.nodes[node_name].update(
            {k: v for k, v in msg["data"].items() if k != "node_name"}
        )
        self.nodes[node_name]["response"] = True

        # Construct information of all the nodes to be send to the Manager
        # nodes_status_data = {
        #     "name": self.name,
        #     "nodes_status": {k: self.nodes[k]["status"] for k in self.nodes},
        # }

        # Update Manager on the new nodes status
        # if self.connected_to_manager:
        #     await self.client.async_send(
        #         signal=WORKER_MESSAGE.REPORT_NODES_STATUS,
        #         data=nodes_status_data,
        #     )

    ####################################################################
    ## Helper Methods
    ####################################################################

    def mark_response_as_false_for_node(self, node_name: str):
        self.nodes[node_name]["response"] = False

    def mark_all_response_as_false_for_nodes(self):

        for node_name in self.nodes:
            self.mark_response_as_false_for_node(node_name)

    async def wait_until_node_response(
        self,
        node_name: str,
        timeout: Union[float, int] = 10,
        attribute: str = "response",
        status: Optional[bool] = None,
    ) -> bool:

        # # Wait until the node has informed us that it has been initialized
        if status:
            return await async_waiting_for(
                condition=lambda: self.nodes[node_name]["status"][attribute] == True,
                check_period=0.1,
                timeout=timeout,
            )
        else:
            return await async_waiting_for(
                condition=lambda: self.nodes[node_name][attribute] == True,
                check_period=0.1,
                timeout=timeout,
            )

    async def wait_until_all_nodes_responded(
        self,
        timeout: Union[float, int] = 10,
        attribute: str = "response",
        status: Optional[bool] = None,
    ) -> bool:

        for node_name in self.nodes:
            success = await self.wait_until_node_response(
                node_name, timeout, attribute, status
            )
            if not success:
                logger.debug(f"{self}: Node {node_name} responding: FAILED")
                return False

        logger.debug(f"{self}: All Nodes responding: SUCCESS")
        return True

    def create_node_server_data(self):

        # Construct simple data structure for Node to address information
        node_server_data = {"name": self.name, "nodes": {}}
        for node_name, node_data in self.nodes.items():
            node_server_data["nodes"][node_name] = {
                "host": node_data["host"],
                "port": node_data["port"],
            }

        return node_server_data

    def exec_coro(self, coro: Coroutine):
        self.server._thread.exec(coro)

    ####################################################################
    ## Worker Sync Lifecycle API
    ####################################################################

    def connect(self, host: str, port: int, timeout: Union[int, float] = 10.0) -> bool:
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
            bool: Success in connecting to the Manager

        """

        # Sending message to register
        r = requests.post(
            f"http://{host}:{port}/workers/register",
            data=json.dumps(
                {
                    "name": self.name,
                    "register": True,
                    "addr": socket.gethostbyname(socket.gethostname()),
                    "http_port": self.port,
                    "http_ip": self.host,
                }
            ),
            timeout=config.get("worker.timeout.info-request"),
        )

        # Check if success
        if r.status_code == requests.codes.ok:

            # Update the configuration of the Worker
            config.update_defaults(r.json())

            # Tracking the state and location of the manager
            self.connected_to_manager = True
            self.manager_host = host
            self.manager_port = port
            self.manager_url = f"http://{host}:{port}/workers/register"
            logger.info(
                f"{self}: connection successful to Manager located at {host}:{port}."
            )
            return True

        return False

    def deregister(self):

        r = requests.post(
            self.manager_url,
            data=json.dumps(
                {
                    "name": self.name,
                    "register": False,
                    "addr": socket.gethostbyname(socket.gethostname()),
                }
            ),
            timeout=config.get("worker.timeout.info-request"),
        )

        self.connected_to_manager = False

        return r.status_code == requests.codes.ok

    def create_node(self, msg: Dict[str, Any]):

        # Saving name to track it for now
        node_name = msg["node_name"]
        logger.debug(f"{self}: received request for Node {node_name} creation: {msg}")

        # Saving the node data
        self.nodes[node_name] = {k: v for k, v in msg.items() if k != "node_name"}
        self.nodes[node_name]["status"] = {
            "INIT": 0,
            "CONNECTED": 0,
            "READY": 0,
            "FINISHED": 0,
        }
        self.nodes[node_name]["response"] = False
        self.nodes[node_name]["gather"] = None

        # Keep trying to start a process until success
        success = False
        for i in range(config.get("worker.allowed-failures")):

            # Decode the node object
            self.nodes[node_name]["node_object"] = dill.loads(
                self.nodes[node_name]["pickled"]
            )

            # Provide configuration information to the node once in the client
            self.nodes[node_name]["node_object"].config(
                self.host,
                self.port,
                self.tempfolder,
                self.nodes[node_name]["in_bound"],
                self.nodes[node_name]["out_bound"],
                self.nodes[node_name]["follow"],
                logging_level=logger.level,
                worker_logging_port=self.log_receiver.port,
            )

            # Before starting, over write the pid
            self.nodes[node_name]["node_object"]._parent_pid = os.getpid()

            # Start the node
            self.nodes[node_name]["node_object"].start()
            logger.debug(f"{self}: started <Node {node_name}>")

            # Wait until response from node
            success = waiting_for(
                condition=lambda: self.nodes[node_name]["response"] == True,
                timeout=config.get("worker.timeout.node-creation"),
            )

            if success:
                logger.debug(f"{self}: {node_name} responding, SUCCESS")
            else:
                # Handle failure
                logger.debug(f"{self}: {node_name} responding, FAILED, retry")
                self.nodes[node_name]["node_object"].shutdown()
                self.nodes[node_name]["node_object"].terminate()
                continue

            # Now we wait until the node has fully initialized and ready-up
            success = waiting_for(
                condition=lambda: self.nodes[node_name]["status"]["READY"] == True,
                timeout=config.get("worker.timeout.info-request"),
            )

            if success:
                logger.debug(f"{self}: {node_name} fully ready, SUCCESS")
                break
            else:
                # Handle failure
                logger.debug(f"{self}: {node_name} fully ready, FAILED, retry")
                self.nodes[node_name]["node_object"].shutdown()
                self.nodes[node_name]["node_object"].terminate()

        if not success:
            logger.error(f"{self}: Node {node_name} failed to create")
        else:
            # Mark success
            logger.debug(f"{self}: completed node creation: {self.nodes}")

        return success

    def step(self):

        # Worker tell all nodes to take a step
        self.server.broadcast(signal=WORKER_MESSAGE.REQUEST_STEP, data={})

    def start_nodes(self):

        # Send message to nodes to start
        self.server.broadcast(signal=WORKER_MESSAGE.START_NODES, data={})

    def stop_nodes(self):

        # Send message to nodes to start
        self.server.broadcast(signal=WORKER_MESSAGE.STOP_NODES, data={})

    def idle(self):

        while not self.has_shutdown:
            time.sleep(2)

    def shutdown(self, msg: Dict = {}):
        """Shutdown ``Worker`` safely.

        The ``Worker`` needs to shutdown its server, client and ``Nodes``
        in a safe manner, such as setting flag variables and clearing
        out queues.

        Args:
            msg (Dict): Leave empty, required to work when ``Manager`` sends\
            shutdown message to ``Worker``.

        """
        # Check if shutdown has been called already
        if self.has_shutdown:
            logger.debug(f"{self}: requested to shutdown when already shutdown.")
            return
        else:
            self.has_shutdown = True

        # Shutdown the Worker 2 Node server
        self.server.shutdown()

        # Shutdown nodes from the client (start all shutdown)
        for node_name in self.nodes:
            self.nodes[node_name]["node_object"].shutdown()

        # Then wait until close, or force
        for node_name in self.nodes:
            self.nodes[node_name]["node_object"].join(
                timeout=config.get("worker.timeout.node-shutdown")
            )

            # If that doesn't work, terminate
            if self.nodes[node_name]["node_object"].exitcode != 0:
                logger.warning(f"{self}: Node {node_name} forced shutdown")
                self.nodes[node_name]["node_object"].terminate()

            logger.debug(f"{self}: Nodes have joined")

        # Stop the log listener
        self.log_receiver.shutdown()
        self.log_receiver.join()

        # Sending message to Manager that client is shutting down (only
        # if the manager hasn't already set the client to not running)
        if self.connected_to_manager:
            try:
                self.deregister()
            except requests.ConnectionError:
                logger.warning(f"{self}: shutdown didn't reach Manager")

        # Delete temp folder if requested
        if self.tempfolder.exists() and self.delete_temp:
            shutil.rmtree(self.tempfolder)

    def __del__(self):

        # Also good to shutdown anything that isn't
        if not self.has_shutdown:
            self.shutdown()
