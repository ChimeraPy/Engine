from typing import Dict, List, Any, Optional, Union, Literal
from multiprocessing.process import AuthenticationString
import time
import socket
import logging
import queue
import uuid
import pathlib
import os
import tempfile
from concurrent.futures import wait

# Third-party Imports
import multiprocess as mp
import numpy as np
import pandas as pd

# Internal Imports
from .networking import Client, Publisher, Subscriber
from .networking.enums import GENERAL_MESSAGE, WORKER_MESSAGE, NODE_MESSAGE
from .data_handlers import SaveHandler
from .utils import clear_queue
from . import _logger


class Node(mp.Process):
    def __init__(self, name: str, debug: Optional[Literal["step", "stream"]] = None):
        """Create a basic unit of computation in ChimeraPy.

        A node has three main functions that can be overwritten to add
        desired behavior: ``prep``, ``step``, and ``teardown``. You don't
        require them all if not necessary. The ``step`` function is executed
        within a while loop, when new inputs are available (if inputs are
        specified in the graph).

        If the ``step`` function is too restrictive, the ``main``
        (containing the while loop) can be overwritten instead.

        Args:
            name (str): The name that will later used to refer to the Node.

        """
        super().__init__(daemon=True)

        # Saving input parameters
        self._context = mp.get_start_method()
        self.name = name
        self.status = {"INIT": 0, "CONNECTED": 0, "READY": 0, "FINISHED": 0}

        # If used for debugging, that means that it is being executed
        # not in an external process
        self.debug = debug
        if type(self.debug) != type(None):

            # Get the logger and setup the folder to store output data
            self.logger = _logger.getLogger("chimerapy")
            temp_folder = pathlib.Path(tempfile.mkdtemp())
            self.logger.info(
                f"Debug Mode for Node: Generated data is stored in {temp_folder}"
            )

            # Prepare the node to be used
            self.config(
                "0.0.0.0", 9000, temp_folder, [], [], follow=None, networking=False
            )

            # Only execute this if step debugging
            if self.debug == "step":
                self._prep()
                self.prep()

    ####################################################################
    ## Process Pickling Methods
    ####################################################################

    def __repr__(self):
        return f"<Node {self.name}>"

    def __str__(self):
        return self.__repr__()

    def __getstate__(self):
        """called when pickling

        This hack allows subprocesses to be spawned without the
        AuthenticationString raising an error

        """
        state = self.__dict__.copy()
        conf = state["_config"]
        if "authkey" in conf:
            # del conf['authkey']
            conf["authkey"] = bytes(conf["authkey"])
        return state

    def __setstate__(self, state):
        """for unpickling"""
        state["_config"]["authkey"] = AuthenticationString(state["_config"]["authkey"])
        self.__dict__.update(state)

    def get_logger(self) -> logging.Logger:

        # If running in a the main process
        if "MainProcess" in mp.current_process().name:
            l = _logger.getLogger("chimerapy")
        else:
            # Depending on the type of process, get the self.logger
            if self._context == "spawn":
                l = _logger.getLogger("chimerapy-subprocess")
            elif self._context == "fork":
                l = _logger.getLogger("chimerapy-subprocess")
            else:
                raise RuntimeError("Invalid multiprocessing spawn method.")

        return l

    ####################################################################
    ## Message Reactivity API
    ####################################################################

    async def process_node_server_data(self, msg: Dict):

        # We determine all the out bound nodes
        for out_bound_name in self.p2p_info["out_bound"]:

            self.logger.debug(f"{self}: Setting up clients: {self.name}: {msg}")

            # Determine the host and port information
            out_bound_info = msg["data"][out_bound_name]

            # Create a client to the out bound node
            p2p_client = Client(
                host=out_bound_info["host"],
                port=out_bound_info["port"],
                name=str(self),
                ws_handlers=self.to_node_handlers,
            )

            # # Save the client
            self.p2p_clients[out_bound_name] = p2p_client

        # Notify to the worker that the node is fully CONNECTED
        self.status["CONNECTED"] = 1
        self.client.send(
            signal=NODE_MESSAGE.STATUS,
            data={
                "node_name": self.name,
                "status": self.status,
            },
        )

    async def provide_gather(self, msg: Dict):

        self.client.send(
            signal=NODE_MESSAGE.REPORT_GATHER,
            data={
                "node_name": self.name,
                "latest_value": self.latest_value,
            },
        )

    async def start_node(self, msg: Dict):
        self.worker_signal_start = True
        self.logger.debug(f"{self}: start")

    async def stop_node(self, msg: Dict):
        self.running.value = False

    ####################################################################
    ## Saving Data Stream API
    ####################################################################

    def save_video(self, name: str, data: np.ndarray, fps: int):
        video_chunk = {
            "uuid": uuid.uuid4(),
            "name": name,
            "data": data,
            "dtype": "video",
            "fps": fps,
        }
        self.save_queue.put(video_chunk)

    def save_audio(
        self, name: str, data: np.ndarray, channels: int, format: int, rate: int
    ):
        audio_chunk = {
            "uuid": uuid.uuid4(),
            "name": name,
            "data": data,
            "dtype": "audio",
            "channels": channels,
            "format": format,
            "rate": rate,
        }
        self.save_queue.put(audio_chunk)

    def save_tabular(
        self, name: str, data: Union[pd.DataFrame, Dict[str, Any], pd.Series]
    ):
        tabular_chunk = {
            "uuid": uuid.uuid4(),
            "name": name,
            "data": data,
            "dtype": "tabular",
        }
        self.save_queue.put(tabular_chunk)

    def save_image(self, name: str, data: np.ndarray):
        image_chunk = {
            "uuid": uuid.uuid4(),
            "name": name,
            "data": data,
            "dtype": "image",
        }
        self.save_queue.put(image_chunk)

    ####################################################################
    ## Node Lifecycle API
    ####################################################################

    def config(
        self,
        host: str,
        port: int,
        logdir: pathlib.Path,
        in_bound: List[str],
        out_bound: List[str],
        follow: Optional[bool],
        networking: bool = True,
    ):
        """Configuring the ``Node``'s networking and meta data.

        This function does not create the connections between the ``Node``
        and the ``Worker``, as that is done in the ``_prep`` method. It
        just inplants the networking meta from the ``Worker`` to the
        ``Node``. This is because we have to instantiate the ``Server``,
        ``Client``, and other components of the ``Node`` inside the ``run``
        method.

        Args:
            host (str): Worker's host
            port (int): Worker's port
            in_bound (List[str]): List of node names that will be inputs
            out_bound (List[str]): List of node names that will be sent the output
            networking (bool): Optional deselect networking setup (used in testing)

        """
        # Obtaining worker information
        self.worker_host = host
        self.worker_port = port
        self.logdir = logdir / self.name
        os.makedirs(self.logdir, exist_ok=True)

        # Storing p2p information
        self.p2p_info = {"in_bound": in_bound, "out_bound": out_bound}
        self.follow = follow

        # Keeping track of the node's state
        self.running = mp.Value("i", True)

        # Saving other parameters
        self.networking = networking

        # Creating initial values
        self.latest_value = None

    def _prep(self):
        """Establishes the connection between ``Node`` and ``Worker``

        The client that connects the ``Node`` to the ``Worker`` is created
        within this function, along with ``Node`` meta data.

        """
        # Create container for p2p clients
        self.p2p_clients = {}

        # Create input and output queue
        self.save_queue = queue.Queue()

        # Creating thread for saving incoming data
        self.save_handler = SaveHandler(logdir=self.logdir, save_queue=self.save_queue)
        self.save_handler.start()

        # Keeping parameters
        self.step_id = 0
        self.worker_signal_start = 0
        self.status["INIT"] = 1

        if self.networking:

            # Create client to the Worker
            self.client = Client(
                host=self.worker_host,
                port=self.worker_port,
                name=str(self),
                ws_handlers={
                    GENERAL_MESSAGE.SHUTDOWN: self.shutdown,
                    WORKER_MESSAGE.BROADCAST_NODE_SERVER_DATA: self.process_node_server_data,
                    WORKER_MESSAGE.REQUEST_STEP: self.forward,
                    WORKER_MESSAGE.REQUEST_GATHER: self.provide_gather,
                    WORKER_MESSAGE.START_NODES: self.start_node,
                    WORKER_MESSAGE.STOP_NODES: self.stop_node,
                },
            )
            self.client.connect()

    def prep(self):
        """User-defined method for ``Node`` setup.

        In this method, the setup logic of the ``Node`` is executed. This
        would include opening files, creating connections to sensors, and
        calibrating sensors.

        """
        ...

    def ready(self):

        # Notify to the worker that the node is fully READY
        self.status["READY"] = 1
        if self.networking:
            self.client.send(
                signal=NODE_MESSAGE.STATUS,
                data={
                    "node_name": self.name,
                    "status": self.status,
                },
            )

    def waiting(self):

        # Only wait if connected to Worker
        if self.networking:

            # Wait until worker says to start
            while self.running.value:
                if self.worker_signal_start:
                    break
                else:
                    time.sleep(0.1)

    def forward(self, msg: Dict):

        # If no in_bound, just send data
        if len(self.p2p_info["in_bound"]) == 0:
            output = self.step()

        else:

            # Else, we have to wait for inputs
            while self.running.value:

                # Using a light-weight data ready notification queue to
                # leverage propery blocking
                try:
                    ready = self.in_data_ready_notification_queue.get(timeout=2)
                except queue.Empty:
                    continue

                if ready:
                    inputs = self.in_bound_data.copy()
                    self.new_data_available = False
                else:
                    continue

                self.logger.debug(f"{self}: got inputs")

                # Once we get them, pass them through!
                output = self.step(inputs)
                break

        # If output generated, send it!
        if type(output) != type(None):

            # Send out the output to the OutputsHandler
            self.logger.debug(f"{self}: placed data to out_queue")
            self.out_queue.put({"step_id": self.step_id, "data": output})

            # And then save the latest value
            self.latest_value = output

        # Update the counter
        self.step_id += 1

    def step(self, data_dict: Optional[Dict[str, Any]] = None):
        """User-define method.

        In this method, the logic that is executed within the ``Node``'s
        while loop. For data sources (no inputs), the ``step`` method
        will execute as fast as possible; therefore, it is important to
        add ``time.sleep`` to specify the sampling rate.

        For a ``Node`` that have inputs, these will be executed when new
        data is received.

        Args:
            data_dict (Optional[Dict[str, Any]]): For source nodes, this \
            parameter should not be considered (as they don't have inputs).\
            For step and sink nodes, the ``data_dict`` must be included\
            to avoid an error. The variable is a dictionary, where the\
            key is the in-bound ``Node``'s name and the value is the\
            output of the in-bound ``Node``'s ``step`` function.

        """
        ...

    def main(self):
        """User-possible overwritten method.

        This method can also be overwritten, through it is recommend to
        do so carefully. If overwritten, the handling of inputs will have
        to be implemented as well.

        One can have access to this information from ``self.in_bound_data``,
        and ``self.new_data_available`` attributes.

        """
        while self.running.value:
            self.forward({})

    def teardown(self):
        """User-define method.

        This method provides a convienient way to shutdown services, such
        as closing files, signaling to sensors to stop, and making any
        last minute corrections to the data.

        """
        ...

    def _teardown(self):

        # Shutdown the inputs and outputs threads
        self.save_handler.shutdown()
        self.save_handler.join()
        time.sleep(1)

        # Shutting down networking
        if self.networking:

            # Inform the worker that the Node has finished its saving of data
            self.status["FINISHED"] = 1
            self.client.send(
                signal=NODE_MESSAGE.STATUS,
                data={
                    "node_name": self.name,
                    "status": self.status,
                },
            )

            # Shutdown the client
            self.client.shutdown()

    def run(self):
        """The actual method that is executed in the new process.

        When working with ``multiprocessing.Process``, it should be
        considered that the creation of a new process can yield
        unexpected behavior if not carefull. It is recommend that one
        reads the ``mutliprocessing`` documentation to understand the
        implications.

        """
        self.logger = self.get_logger()
        self.logger.debug(f"{self}: initialized with context -> {self._context}")

        # If debugging, do not re-execute the networking component
        # if not self.debug:

        # Performing preparation for the while loop
        self._prep()
        self.prep()

        self.logger.debug(f"{self}: finished prep")

        # Signal that Node is ready
        self.ready()
        self.logger.debug(f"{self}: is ready")
        self.waiting()

        self.logger.debug(f"{self}: is running")
        self.main()

        self.logger.debug(f"{self}: is exiting")
        self.teardown()
        self._teardown()

        self.logger.debug(f"{self}: finished teardown")

    def shutdown(self, msg: Dict = {}):

        self.running.value = False

        # If for debugging, user's don't need to know about _teardown
        # Also, the networking components are not being used
        if self.debug == "step":
            self._teardown()
