from typing import Union, Dict, Any, Coroutine
import socket
import os
import time
import tempfile
import pathlib
import shutil
import sys

# Third-party Imports
import dill
import aiohttp
from aiohttp import web

from .utils import get_ip_address, waiting_for, async_waiting_for
from .networking import Server, Client
from .networking.enums import (
    MANAGER_MESSAGE,
    NODE_MESSAGE,
    WORKER_MESSAGE,
)
from . import _logger

logger = _logger.getLogger("chimerapy")


class Worker:
    def __init__(self, name: str, port: int = 9080, delete_temp: bool = True):
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

    def __repr__(self):
        return f"<Worker {self.name}>"

    def __str__(self):
        return self.__repr__()

    ####################################################################
    ## Message Reactivity API - Client
    ####################################################################

    async def async_create_node(self, msg: Dict):
        self.create_node(msg)

    async def async_step(self, msg: Dict):

        # Worker tell all nodes to take a step
        await self.server.async_broadcast(signal=WORKER_MESSAGE.REQUEST_STEP, data={})

    async def report_node_server_data(self, msg: Dict):

        node_server_data = self.create_node_server_data()

        # Then manager request the node server data, so provide it
        if self.connected_to_manager:
            await self.client.async_send(
                signal=WORKER_MESSAGE.REPORT_NODE_SERVER_DATA,
                data=node_server_data,
            )

    async def process_node_server_data(self, msg: Dict):

        logger.debug(f"{self}: processing node server data")

        await self.server.async_broadcast(
            signal=WORKER_MESSAGE.BROADCAST_NODE_SERVER_DATA,
            data=msg["data"],
        )

        # Now wait until all nodes have responded as CONNECTED
        self.wait_until_all_nodes_responded()
        logger.debug(f"{self}: Nodes have been connected.")

        # After all nodes have been connected, inform the Manager
        nodes_status_data = {
            "name": self.name,
            "nodes_status": {k: self.nodes[k]["status"] for k in self.nodes},
        }

        logger.debug(f"{self}: Informing Manager of processing completion")

        # Update the nodes status
        if self.connected_to_manager:
            await self.client.async_send(
                signal=WORKER_MESSAGE.REPORT_NODES_STATUS,
                data=nodes_status_data,
            )

            # Then confirm that all the node server data has been distributed
            await self.client.async_send(
                signal=WORKER_MESSAGE.COMPLETE_BROADCAST, data={"name": self.name}
            )

    async def report_node_gather(self, msg: Dict):

        logger.debug(f"{self}: reporting to Manager gather request")

        # Marking as all false
        self.mark_all_response_as_false_for_nodes()

        # Request gather from Worker to Nodes
        await self.server.async_broadcast(signal=WORKER_MESSAGE.REQUEST_GATHER, data={})

        self.wait_until_all_nodes_responded(timeout=5)

        # Gather the data from the nodes!
        gather_data = {"name": self.name, "node_data": {}}
        for node_name, node_data in self.nodes.items():
            gather_data["node_data"][node_name] = node_data["gather"]

        # Send it back to the Manager
        await self.client.async_send(
            signal=WORKER_MESSAGE.REPORT_GATHER, data=gather_data
        )

    async def load_sent_packages(self, msg: Dict):

        # For each package, extract it from the client's tempfolder
        # and load it to the sys.path
        for sent_package in msg["data"]["packages"]:

            # Wait until the sent package are started
            await async_waiting_for(
                condition=lambda: f"{sent_package}.zip"
                in self.server.file_transfer_records["Manager"],
                check_period=0.5,
                success_msg=f"{self}: Started gettng sent package: {sent_package}",
                timeout=60,
                timeout_raise=True,
                timeout_msg=f"{self}: Package {sent_package} was not obtained at all.",
            )

            # Get the path
            package_zip_path = self.server.file_transfer_records["Manager"][
                f"{sent_package}.zip"
            ]["dst_filepath"]

            # Wait until the sent package is complete
            await async_waiting_for(
                condition=lambda: self.server.file_transfer_records["Manager"][
                    f"{sent_package}.zip"
                ]["complete"]
                == True,
                check_period=0.5,
                success_msg=f"{self}: Got package {sent_package}",
                timeout=60,
                timeout_raise=True,
                timeout_msg=f"{self}: Package {sent_package} was not fully received, therefore failing to load.",
            )

            assert (
                package_zip_path.exists()
            ), f"{self}: {package_zip_path} doesn't exists!?"
            sys.path.insert(0, str(package_zip_path))

        # Send message back to the Manager letting them know that
        logger.info(f"{self}: Completed loading packages sent by Manager")
        await self.client.async_send(
            signal=WORKER_MESSAGE.PACKAGE_LOADED, data={"name": self.name}
        )

    async def send_archive(self, msg: Dict):

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
                    shutil.move(self.tempfolder, pathlib.Path(msg["data"]["path"]))
                    break
                except shutil.Error:  # File already exists!
                    break
                except:
                    time.sleep(delay)
                    miss_counter += 1
                    if miss_counter * delay > timeout:
                        raise TimeoutError("Nodes haven't fully finishing saving!")

            old_folder_name = pathlib.Path(msg["data"]["path"]) / self.tempfolder.name
            new_folder_name = pathlib.Path(msg["data"]["path"]) / self.name
            os.rename(old_folder_name, new_folder_name)

        else:

            logger.debug(f"{self}: sending archive via network")

            # Else, send the archive data to the manager via network
            try:
                await self.client._send_folder_async(self.name, self.tempfolder)
                success = True
            except (TimeoutError, SystemError) as error:
                self.delete_temp = False
                logger.exception(
                    f"{self}: Failed to transmit files to Manager - {error}."
                )
                success = False

        # After completion, let the Manager know
        await self.client.async_send(
            signal=WORKER_MESSAGE.TRANSFER_COMPLETE,
            data={"name": self.name, "success": success},
        )

    ####################################################################
    ## Message Reactivity API - Server
    ####################################################################

    async def node_report_gather(self, msg: Dict, ws: web.WebSocketResponse):

        # Saving name to track it for now
        node_name = msg["data"]["node_name"]
        self.nodes[node_name]["response"] = True
        self.nodes[node_name]["gather"] = msg["data"]["latest_value"]

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
        nodes_status_data = {
            "name": self.name,
            "nodes_status": {k: self.nodes[k]["status"] for k in self.nodes},
        }

        # Update Manager on the new nodes status
        if self.connected_to_manager:
            await self.client.async_send(
                signal=WORKER_MESSAGE.REPORT_NODES_STATUS,
                data=nodes_status_data,
            )

    ####################################################################
    ## Helper Methods
    ####################################################################

    def mark_response_as_false_for_node(self, node_name: str):
        self.nodes[node_name]["response"] = False

    def mark_all_response_as_false_for_nodes(self):

        for node_name in self.nodes:
            self.mark_response_as_false_for_node(node_name)

    def wait_until_node_response(self, node_name: str, timeout: Union[float, int] = 10):

        # # Wait until the node has informed us that it has been initialized
        waiting_for(
            condition=lambda: self.nodes[node_name]["response"] == True,
            check_period=0.1,
            timeout=timeout,
            timeout_raise=True,
            timeout_msg=f"{self}: {node_name} not responding!",
        )

    def wait_until_all_nodes_responded(self, timeout: Union[float, int] = 10):

        for node_name in self.nodes:
            self.wait_until_node_response(node_name, timeout)

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
    ## Worker ASync Lifecycle API
    ####################################################################

    async def async_start_nodes(self, msg: Dict):

        # Send message to nodes to start
        await self.server.async_broadcast(signal=WORKER_MESSAGE.START_NODES, data={})

    async def async_stop_nodes(self, msg: Dict):

        # Send message to nodes to start
        await self.server.async_broadcast(signal=WORKER_MESSAGE.STOP_NODES, data={})

    async def async_shutdown(self, msg: Dict):
        self.shutdown()

    ####################################################################
    ## Worker Sync Lifecycle API
    ####################################################################

    def connect(self, host: str, port: int, timeout: Union[int, float] = 10.0):
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

        """
        # Create client
        self.client = Client(
            host=host,
            port=port,
            name=self.name,
            ws_handlers={
                MANAGER_MESSAGE.CREATE_NODE: self.async_create_node,
                MANAGER_MESSAGE.REQUEST_NODE_SERVER_DATA: self.report_node_server_data,
                MANAGER_MESSAGE.BROADCAST_NODE_SERVER_DATA: self.process_node_server_data,
                MANAGER_MESSAGE.REQUEST_GATHER: self.report_node_gather,
                MANAGER_MESSAGE.REQUEST_STEP: self.async_step,
                MANAGER_MESSAGE.START_NODES: self.async_start_nodes,
                MANAGER_MESSAGE.STOP_NODES: self.async_stop_nodes,
                MANAGER_MESSAGE.REQUEST_COLLECT: self.send_archive,
                MANAGER_MESSAGE.REQUEST_CODE_LOAD: self.load_sent_packages,
                MANAGER_MESSAGE.CLUSTER_SHUTDOWN: self.async_shutdown,
            },
        )
        self.client.connect()

        # Sending message to register
        self.client.send(
            signal=WORKER_MESSAGE.REGISTER,
            data={
                "name": self.name,
                "addr": socket.gethostbyname(socket.gethostname()),
                "http_port": self.port,
                "http_ip": self.host,
            },
            ok=True,
        )

        # Tracking client state change
        self.connected_to_manager = True
        self.manager_host = host
        logger.info(
            f"{self}: connection successful to Manager located at {host}:{port}."
        )

    def create_node(self, msg: Dict[str, Any]):

        # Saving name to track it for now
        node_name = msg["data"]["node_name"]
        logger.debug(f"{self}: received request for Node {node_name} creation: {msg}")

        # Saving the node data
        self.nodes[node_name] = {
            k: v for k, v in msg["data"].items() if k != "node_name"
        }
        self.nodes[node_name]["status"] = {"INIT": 0, "CONNECTED": 0, "READY": 0}
        self.nodes[node_name]["response"] = False
        self.nodes[node_name]["gather"] = None

        # Keep trying to start a process until success
        fail_attempts = 0
        while True:

            # If too many attempts, just give up :(
            if fail_attempts > 5:
                raise TimeoutError("Could not create Node")

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
            )

            # Before starting, over write the pid
            self.nodes[node_name]["node_object"]._parent_pid = os.getpid()

            # Start the node
            self.nodes[node_name]["node_object"].start()

            # Wait until response from node
            try:
                self.wait_until_node_response(node_name, timeout=10)
                break
            except TimeoutError:

                # Handle failure
                self.nodes[node_name]["node_object"].shutdown()
                self.nodes[node_name]["node_object"].terminate()
                fail_attempts += 1
                logger.warning(
                    f"{node_name} failed to start, retrying for {fail_attempts} time."
                )

        # Mark success
        logger.debug(f"{self}: completed node creation: {self.nodes}")

        # Update the manager with the most up-to-date status of the nodes
        nodes_status_data = {
            "name": self.name,
            "nodes_status": {k: self.nodes[k]["status"] for k in self.nodes},
        }

        if self.connected_to_manager:
            self.client.send(
                signal=WORKER_MESSAGE.REPORT_NODES_STATUS, data=nodes_status_data
            )

    def step(self, msg: Dict = {}):

        # Worker tell all nodes to take a step
        self.server.broadcast(signal=WORKER_MESSAGE.REQUEST_STEP, data={})

    def start_nodes(self, msg: Dict = {}):

        # Send message to nodes to start
        self.server.broadcast(signal=WORKER_MESSAGE.START_NODES, data={})

    def stop_nodes(self, msg: Dict = {}):

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
            self.nodes[node_name]["node_object"].join(timeout=10)

            # If that doesn't work, terminate
            if self.nodes[node_name]["node_object"].exitcode != 0:
                logger.warning(f"{self}: Node {node_name} forced shutdown")
                self.nodes[node_name]["node_object"].terminate()

            logger.debug(f"{self}: Nodes have joined")

        # Sending message to Manager that client is shutting down (only
        # if the manager hasn't already set the client to not running)
        if self.connected_to_manager:
            self.client.send(
                signal=WORKER_MESSAGE.DEREGISTER,
                data={
                    "name": self.name,
                    "addr": socket.gethostbyname(socket.gethostname()),
                },
            )

        # Delete temp folder if requested
        if self.tempfolder.exists() and self.delete_temp:
            shutil.rmtree(self.tempfolder)

    def __del__(self):

        # Also good to shutdown anything that isn't
        if not self.has_shutdown:
            self.shutdown()
