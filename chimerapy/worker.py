from typing import Union, Dict
import multiprocessing as mp
import socket
import logging
import os
import time
import pdb
import tempfile
import pathlib
import shutil
import sys

import dill

from .server import Server
from .client import Client
from . import enums
from .utils import get_ip_address
from . import _logger

logger = _logger.getLogger("chimerapy")


class Worker:
    def __init__(self, name: str, max_num_of_nodes: int = 10, delete_temp: bool = True):
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
            max_num_of_nodes (int): Maximum number of ``Nodes`` supported\
            by the constructed ``Worker``.
        """

        # Saving parameters
        self.name = name
        self.max_num_of_nodes = max_num_of_nodes

        # Instance variables
        self.has_shutdown: bool = False
        self.nodes: Dict = {}
        self.manager_ack: bool = False
        self.connected_to_manager: bool = False
        self.manager_host = "0.0.0.0"

        # Indicating which function to response
        self.to_manager_handlers = {
            enums.SHUTDOWN: self.shutdown,
            enums.MANAGER_CREATE_NODE: self.create_node,
            enums.MANAGER_REQUEST_NODE_SERVER_DATA: self.report_node_server_data,
            enums.MANAGER_BROADCAST_NODE_SERVER_DATA: self.process_node_server_data,
            enums.MANAGER_REQUEST_GATHER: self.report_node_gather,
            enums.MANAGER_REQUEST_STEP: self.step,
            enums.MANAGER_START_NODES: self.start_nodes,
            enums.MANAGER_STOP_NODES: self.stop_nodes,
            enums.MANAGER_REQUEST_COLLECT: self.send_archive,
            enums.MANAGER_REQUEST_CODE_LOAD: self.load_sent_packages,
        }
        self.from_node_handlers = {
            enums.NODE_STATUS: self.node_status_update,
            enums.NODE_REPORT_GATHER: self.node_report_gather,
        }

        # Create temporary data folder
        self.delete_temp = delete_temp
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())

        # Create server
        self.server = Server(
            port=8000,
            name=f"Worker {self.name}",
            max_num_of_clients=self.max_num_of_nodes,
            sender_msg_type=enums.WORKER_MESSAGE,
            accepted_msg_type=enums.NODE_MESSAGE,
            handlers=self.from_node_handlers,
        )

        # Accesing the port information and starting the server
        self.host, self.port = self.server.host, self.server.port
        self.server.start()

    def __repr__(self):
        return f"<Worker {self.name}>"

    def __str__(self):
        return self.__repr__()

    ####################################################################
    ## Message Reactivity API
    ####################################################################

    def create_node(self, msg: Dict):

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
                {
                    "signal": enums.WORKER_REPORT_NODES_STATUS,
                    "data": nodes_status_data,
                },
            )

    def report_node_server_data(self, msg: Dict):

        node_server_data = self.create_node_server_data()

        # Then manager request the node server data, so provide it
        if self.connected_to_manager:
            self.client.send(
                {
                    "signal": enums.WORKER_REPORT_NODE_SERVER_DATA,
                    "data": node_server_data,
                }
            )

    def process_node_server_data(self, msg: Dict):

        logger.debug(f"{self}: processing node server data")

        self.server.broadcast(
            {
                "signal": enums.WORKER_BROADCAST_NODE_SERVER_DATA,
                "data": msg["data"],
            }
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
            self.client.send(
                {
                    "signal": enums.WORKER_REPORT_NODES_STATUS,
                    "data": nodes_status_data,
                },
            )

            # Then confirm that all the node server data has been distributed
            self.client.send(
                {"signal": enums.WORKER_COMPLETE_BROADCAST, "data": {"name": self.name}}
            )

    def node_report_gather(self, msg: Dict, node_socket: socket.socket):

        # Saving name to track it for now
        node_name = msg["data"]["node_name"]
        self.nodes[node_name]["response"] = True
        self.nodes[node_name]["gather"] = msg["data"]["latest_value"]

    def report_node_gather(self, msg: Dict):

        logger.debug(f"{self}: reporting to Manager gather request")

        # Marking as all false
        self.mark_all_response_as_false_for_nodes()

        # Request gather from Worker to Nodes
        self.server.broadcast(
            {
                "signal": enums.WORKER_REQUEST_GATHER,
                "data": {},
            }
        )

        self.wait_until_all_nodes_responded(timeout=5)

        # Gather the data from the nodes!
        gather_data = {"name": self.name, "node_data": {}}
        for node_name, node_data in self.nodes.items():
            gather_data["node_data"][node_name] = node_data["gather"]

        # Send it back to the Manager
        self.client.send({"signal": enums.WORKER_REPORT_GATHER, "data": gather_data})

    def node_status_update(self, msg: Dict, s: socket.socket):

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
            self.client.send(
                {
                    "signal": enums.WORKER_REPORT_NODES_STATUS,
                    "data": nodes_status_data,
                }
            )

    def load_sent_packages(self, msg: Dict):

        # For each package, extract it from the client's tempfolder
        # and load it to the sys.path
        for sent_package in msg["data"]["packages"]:
            package_zip_path = self.client.file_transfer_records["Manager"][
                f"{sent_package}.zip"
            ]
            assert package_zip_path.exists()
            sys.path.insert(0, package_zip_path)

    def send_archive(self, msg: Dict):

        # Default value of success
        success = False

        # If located in the same computer, just move the data
        if self.manager_host == get_ip_address():

            # First rename and then move
            # new_folder_name = self.tempfolder.parent / self.name
            # os.rename(self.tempfolder, new_folder_name)
            # shutil.move(new_folder_name, msg["data"]["path"])
            delay = 1
            miss_counter = 0
            timeout = 10
            while True:
                try:
                    shutil.move(self.tempfolder, msg["data"]["path"])
                    break
                except shutil.Error:  # File already exists!
                    break
                except:
                    time.sleep(delay)
                    miss_counter += 1
                    if miss_counter * delay > timeout:
                        raise TimeoutError("Nodes haven't fully finishing saving!")

            old_folder_name = msg["data"]["path"] / self.tempfolder.name
            new_folder_name = msg["data"]["path"] / self.name
            os.rename(old_folder_name, new_folder_name)

        else:

            # Else, send the archive data to the manager via network
            try:
                self.client.send_folder(self.name, self.tempfolder)
                success = True
            except (TimeoutError, SystemError) as error:
                self.delete_temp = False
                logger.exception(
                    f"{self}: Failed to transmit files to Manager - {error}."
                )
                success = False

        # After completion, let the Manager know
        self.client.send(
            {
                "signal": enums.WORKER_TRANSFER_COMPLETE,
                "data": {"name": self.name, "success": success},
            }
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

        # Wait until the node has informed us that it has been initialized
        miss_counter = 0
        delay = 0.1

        # Constantly check
        while True:
            time.sleep(delay)

            if self.nodes[node_name]["response"]:
                break
            else:

                # Handling timeout
                if miss_counter * delay > timeout:
                    raise TimeoutError(f"{self}: {node_name} not responding!")

                # Update miss counter
                miss_counter += 1

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

    ####################################################################
    ## Node Lifecycle API
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
            name=f"Worker {self.name}",
            connect_timeout=timeout,
            sender_msg_type=enums.WORKER_MESSAGE,
            accepted_msg_type=enums.MANAGER_MESSAGE,
            handlers=self.to_manager_handlers,
        )
        self.client.start()

        # Sending message to register
        self.client.send(
            msg={
                "signal": enums.WORKER_REGISTER,
                "data": {
                    "name": self.name,
                    "addr": socket.gethostbyname(socket.gethostname()),
                },
            },
            ack=True,
        )

        # Tracking client state change
        self.connected_to_manager = True
        self.manager_host = host
        logger.info(
            f"{self}: connection successful to Manager located at {host}:{port}."
        )

    def step(self, msg: Dict):

        # Worker tell all nodes to take a step
        self.server.broadcast({"signal": enums.WORKER_REQUEST_STEP, "data": {}})

    def start_nodes(self, msg: Dict):

        # Send message to nodes to start
        self.server.broadcast({"signal": enums.WORKER_START_NODES, "data": {}})

    def stop_nodes(self, msg: Dict):

        # Send message to nodes to start
        self.server.broadcast({"signal": enums.WORKER_STOP_NODES, "data": {}})

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
            logger.debug(f"{self}: requested to shutdown a second time.")
            return

        # Shutdown the Worker 2 Node server
        self.server.shutdown()

        # Shutdown nodes from the client
        for node_name in self.nodes:

            # Try shutting down process gently
            self.nodes[node_name]["node_object"].shutdown()
            self.nodes[node_name]["node_object"].join(timeout=10)

            # If that doesn't work, terminate
            if self.nodes[node_name]["node_object"].exitcode != 0:
                self.nodes[node_name]["node_object"].terminate()

        logger.debug(f"{self}: Nodes have joined")

        # Sending message to Manager that client is shutting down (only
        # if the manager hasn't already set the client to not running)
        if self.connected_to_manager:
            self.client.send(
                {
                    "type": enums.WORKER_MESSAGE,
                    "signal": enums.WORKER_DEREGISTER,
                    "data": {
                        "name": self.name,
                        "addr": socket.gethostbyname(socket.gethostname()),
                    },
                },
            )

            # Shutdown client
            self.client.shutdown()

        # Delete temp folder if requested
        if self.tempfolder.exists() and self.delete_temp:
            shutil.rmtree(self.tempfolder)

        # Mark that the Worker has shutdown
        self.has_shutdown = True

    def __del__(self):

        # Also good to shutdown anything that isn't
        if not self.has_shutdown:
            self.shutdown()
