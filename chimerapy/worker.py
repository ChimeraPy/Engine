from typing import Union, Dict
import multiprocessing as mp
import socket
import logging
import os
import time
import pdb
import jsonpickle

logger = logging.getLogger("chimerapy")

from .server import Server
from .client import Client
from .utils import log
from . import enums


class Worker:
    def __init__(self, name: str, max_num_of_nodes: int = 10):

        # Saving parameters
        self.name = name
        self.max_num_of_nodes = max_num_of_nodes

        # Instance variables
        self.nodes: Dict = {}
        self.manager_ack: bool = False
        self.connected_to_manager: bool = False

        # Indicating which function to response
        self.to_manager_handlers = {
            enums.SHUTDOWN: self.shutdown,
            enums.MANAGER_CREATE_NODE: self.create_node,
            enums.MANAGER_REQUEST_NODE_SERVER_DATA: self.report_node_server_data,
            enums.MANAGER_BROADCAST_NODE_SERVER_DATA: self.process_node_server_data,
            enums.MANAGER_REQUEST_STEP: self.step,
            enums.MANAGER_START_NODES: self.start_nodes,
            enums.MANAGER_STOP_NODES: self.stop_nodes,
        }
        self.from_node_handlers = {enums.NODE_STATUS: self.node_status_update}

        # Create server
        self.server = Server(
            host=socket.gethostbyname(socket.gethostname()),
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


    def connect(self, host: str, port: int, timeout: Union[int, float] = 10.0):

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
        logger.info(f"{self}: connected to Manager")

    
    def create_node(self, msg: Dict):
        
        # Saving name to track it for now
        node_name = msg["data"]["node_name"]
        logger.info(f"{self}: received request for Node {node_name} creation: {msg}")

        # Saving the node data
        self.nodes[node_name] = {
            k: v for k, v in msg["data"].items() if k != "node_name"
        }
        self.nodes[node_name]["status"] = {"INIT": 0, "CONNECTED": 0, "READY": 0}

        # Decode the node object
        self.nodes[node_name]["node_object"] = jsonpickle.decode(
            self.nodes[node_name]["node_object"]
        )

        # Provide configuration information to the node once in the client
        self.nodes[node_name]["node_object"].config(
            self.host,
            self.port,
            self.nodes[node_name]["in_bound"],
            self.nodes[node_name]["out_bound"],
        )

        # Start the node
        self.nodes[node_name]["node_object"].start()

        # Wait until the node has informed us that it has been initialized
        miss_counter = 0
        while True:
            time.sleep(0.1)

            if self.nodes[node_name]["status"]["INIT"]:
                break
            else:

                if miss_counter > 10:
                    pdb.set_trace()
                    raise RuntimeError(f"{self.name} for {node_name} is not starting!")

                miss_counter += 1

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
                }
            )

    def create_node_server_data(self):

        # Construct simple data structure for Node to address information
        node_server_data = {"name": self.name, "nodes": {}}
        for node_name, node_data in self.nodes.items():
            node_server_data["nodes"][node_name] = {
                "host": node_data["host"],
                "port": node_data["port"],
            }

        return node_server_data

    
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
        for node_name in self.nodes:

            miss_counter = 0
            delay = 0.1
            timeout = 1
            logger.debug(f"{self}: waiting for CONNECTED msg from {node_name}")

            while True:
                if self.nodes[node_name]["status"]["CONNECTED"]:
                    break
                else:
                    time.sleep(delay)
                    
                    if miss_counter * delay > timeout:
                        raise RuntimeError(f"{self}: timeout waiting for CONNECTED msg from {node_name}")

                    miss_counter += 1

            logger.debug(f"{self}: received CONNECTED msg from {node_name}")

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
                }
            )

            # Then confirm that all the node server data has been distributed
            self.client.send(
                {"signal": enums.WORKER_COMPLETE_BROADCAST, "data": {"name": self.name}}
            )

    
    def node_status_update(self, msg: Dict, s: socket.socket):

        # Saving name to track it for now
        node_name = msg["data"]["node_name"]
        status = msg["data"]["status"]

        # Update our records by grabbing all data from the msg
        self.nodes[node_name].update(
            {k: v for k, v in msg["data"].items() if k != "node_name"}
        )

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
            self.client.join()
