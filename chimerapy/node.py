from typing import Dict, List, Any, Optional, Union, Literal, Tuple
from multiprocessing.process import AuthenticationString
import logging
import queue
import uuid
import pathlib
import os
import tempfile
import datetime
import threading
import traceback
import uuid

# Third-party Imports
from dataclasses import dataclass
import multiprocess as mp
import numpy as np
import pandas as pd
import zmq

# Internal Imports
from .states import NodeState
from .networking import Client, Publisher, Subscriber, DataChunk
from .networking.enums import GENERAL_MESSAGE, WORKER_MESSAGE, NODE_MESSAGE
from .data_handlers import SaveHandler
from . import _logger


class Node(mp.Process):
    def __init__(
        self,
        name: str,
        debug: Optional[Literal["step", "stream"]] = None,
        debug_port: Optional[int] = None,
    ):
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
        self.state = NodeState(id=str(uuid.uuid4()), name=name)

        # Saving state variables
        self.publisher: Optional[Publisher] = None
        self.p2p_subs: Dict[str, Subscriber] = {}
        self.socket_to_sub_name_mapping: Dict[zmq.Socket, Tuple[str, str]] = {}
        self.sub_poller = zmq.Poller()
        self.poll_inputs_thread: Optional[threading.Thread] = None
        self.logger: logging.Logger = logging.getLogger("chimerapy")

        # Default values
        self.logging_level: int = logging.INFO

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
                "0.0.0.0",
                9000,
                temp_folder,
                [],
                [],
                [],
                follow=None,
                networking=False,
                logging_level=logging.DEBUG,
                worker_logging_port=debug_port,
            )

            # Only execute this if step debugging
            if self.debug == "step":
                self._prep()
                self.prep()

    ####################################################################
    ## Properties
    ####################################################################

    @property
    def id(self) -> str:
        return self.state.id

    @property
    def name(self) -> str:
        return self.state.name

    ####################################################################
    ## Process Pickling Methods
    ####################################################################

    def __repr__(self):
        return f"<Node name={self.state.name} id={self.state.id}>"

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
        l = _logger.getLogger("chimerapy-node")
        l.setLevel(self.logging_level)
        if self.worker_logging_port:
            _logger.add_node_id_zmq_push_handler(
                l, "127.0.0.1", self.worker_logging_port, self.id
            )
        else:
            _logger.add_console_handler(l)
        return l

    ####################################################################
    ## Message Reactivity API
    ####################################################################

    async def process_node_server_data(self, msg: Dict):

        # We determine all the out bound nodes
        for i, in_bound_id in enumerate(self.p2p_info["in_bound"]):

            self.logger.debug(f"{self}: Setting up clients: {self.state.id}: {msg}")

            # Determine the host and port information
            in_bound_info = msg["data"][in_bound_id]

            # Create subscribers to other nodes' publishers
            p2p_subscriber = Subscriber(
                host=in_bound_info["host"], port=in_bound_info["port"]
            )

            # Storing all subscribers
            self.p2p_subs[in_bound_id] = p2p_subscriber
            self.socket_to_sub_name_mapping[p2p_subscriber._zmq_socket] = (
                self.p2p_info["in_bound_by_name"][i],
                in_bound_id,
            )

        # After creating all subscribers, use a poller to track them all
        for sub in self.p2p_subs.values():
            self.sub_poller.register(sub._zmq_socket, zmq.POLLIN)

        # Then start a thread to read the sub poller
        if self.p2p_info["in_bound"]:
            self.poll_inputs_thread = threading.Thread(target=self.poll_inputs)
            self.poll_inputs_thread.start()

        # Notify to the worker that the node is fully CONNECTED
        self.state.connected = True
        self.connected_ready.set()
        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.state.to_dict()
        )

    async def provide_gather(self, msg: Dict):

        await self.client.async_send(
            signal=NODE_MESSAGE.REPORT_GATHER,
            data={
                "state": self.state.to_dict(),
                "latest_value": self.latest_value.to_json(),
            },
        )

    async def provide_saving(self, msg: Dict):

        # Stop the save handler
        self.save_handler.shutdown()
        self.save_handler.join()
        self.state.finished = True

        await self.client.async_send(
            signal=NODE_MESSAGE.STATUS, data=self.state.to_dict()
        )

    async def start_node(self, msg: Dict):
        self.logger.debug(f"{self}: start")
        self.start_time = datetime.datetime.now()
        self.worker_signal_start.set()

    async def async_forward(self, msg: Dict):
        self.forward(msg)

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
            "timestamp": (datetime.datetime.now() - self.start_time).total_seconds(),
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
        in_bound_by_name: List[str],
        out_bound: List[str],
        follow: Optional[str] = None,
        networking: bool = True,
        logging_level: int = logging.INFO,
        worker_logging_port: int = 5555,
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
        self.logdir = logdir / self.state.name
        self.worker_logging_port = worker_logging_port
        os.makedirs(self.logdir, exist_ok=True)

        # Storing p2p information
        self.p2p_info = {
            "in_bound": in_bound,
            "in_bound_by_name": in_bound_by_name,
            "out_bound": out_bound,
        }
        self.follow = follow

        # Keeping track of the node's state
        self.running = mp.Value("i", True)

        # Saving other parameters
        self.networking = networking
        self.logging_level = logging_level

        # Creating initial values
        self.latest_value = None

        # Timekeeping
        self.start_time = datetime.datetime.now()

    def _prep(self):
        """Establishes the connection between ``Node`` and ``Worker``

        The client that connects the ``Node`` to the ``Worker`` is created
        within this function, along with ``Node`` meta data.

        """
        # Create IO queue
        self.save_queue = queue.Queue()

        # Creating container for the latest values of the subscribers
        self.in_bound_data: Dict[str, Optional[DataChunk]] = {
            x: None for x in self.p2p_info["in_bound_by_name"]
        }
        self.inputs_ready = threading.Event()
        self.inputs_ready.clear()

        # Creating thread for saving incoming data
        self.save_handler = SaveHandler(logdir=self.logdir, save_queue=self.save_queue)
        self.save_handler.start()

        # Keeping parameters
        self.step_id = 0
        self.worker_signal_start = threading.Event()
        self.worker_signal_start.clear()
        self.state.init = True

        if self.networking:

            # Create client to the Worker
            self.client = Client(
                host=self.worker_host,
                port=self.worker_port,
                id=self.state.id,
                ws_handlers={
                    GENERAL_MESSAGE.SHUTDOWN: self.shutdown,
                    WORKER_MESSAGE.BROADCAST_NODE_SERVER_DATA: self.process_node_server_data,
                    WORKER_MESSAGE.REQUEST_STEP: self.async_forward,
                    WORKER_MESSAGE.REQUEST_SAVING: self.provide_saving,
                    WORKER_MESSAGE.REQUEST_GATHER: self.provide_gather,
                    WORKER_MESSAGE.START_NODES: self.start_node,
                    WORKER_MESSAGE.STOP_NODES: self.stop_node,
                },
                parent_logger=self.logger,
            )
            self.client.connect()

            # Creating publisher
            self.publisher = Publisher()
            self.publisher.start()
            self.state.port = self.publisher.port

            # Creating threading event to mark that the Node has been connected
            self.connected_ready = threading.Event()
            self.connected_ready.clear()

            # Send publisher port and host information
            self.client.send(
                signal=NODE_MESSAGE.STATUS,
                data=self.state.to_dict(),
            )

    def prep(self):
        """User-defined method for ``Node`` setup.

        In this method, the setup logic of the ``Node`` is executed. This
        would include opening files, creating connections to sensors, and
        calibrating sensors.

        """
        ...

    def ready(self):

        # Notify to the worker that the node is fully READY
        self.state.ready = True

        # Only do so if connected to Worker and its connected
        if self.networking:
            self.client.send(signal=NODE_MESSAGE.STATUS, data=self.state.to_dict())

    def waiting(self):

        # Only wait if connected to Worker
        if self.networking:

            # Wait until worker says to start
            while self.running.value:
                if self.worker_signal_start.wait(timeout=1):
                    break
                # self.logger.debug(f"{self}: waiting")

        self.logger.debug(f"{self}: finished waiting")

    def poll_inputs(self):

        while self.running.value:

            self.logger.debug(f"{self}: polling inputs")

            # Wait until we get data from any of the subscribers
            events = dict(self.sub_poller.poll())

            # Empty if no events
            if len(events) == 0:
                continue

            self.logger.debug(f"{self}: polling event processing {len(events)}")

            # Default value
            follow_event = False

            # Else, update values
            for s in events:  # socket

                self.logger.debug(f"{self}: processing event {s}")

                # Update
                name, id = self.socket_to_sub_name_mapping[s]  # inbound
                serial_data_chunk = s.recv()
                self.in_bound_data[name] = DataChunk.from_bytes(serial_data_chunk)

                # Update flag if new values are coming from the node that is
                # being followed
                if self.follow == id:
                    follow_event = True

            self.logger.debug(
                f"{self}: polling {self.in_bound_data}, follow = {follow_event}, event= {events}"
            )

            # If update on the follow and all inputs available, then use the inputs
            if follow_event and all(
                [type(x) != type(None) for x in self.in_bound_data.values()]
            ):
                self.inputs_ready.set()
                self.logger.debug(f"{self}: got inputs")

    def forward(self, msg: Dict):

        # Default value
        output = None

        # If no in_bound, just send data
        if len(self.p2p_info["in_bound"]) == 0:
            try:
                output = self.step()
            except Exception as e:
                traceback_info = traceback.format_exc()
                self.logger.error(traceback_info)
                return None

        else:

            # Else, we have to wait for inputs
            while self.running.value:

                self.logger.debug(f"{self}: forward waiting for inputs")

                if self.inputs_ready.wait(timeout=1):
                    # Once we get them, pass them through!
                    self.inputs_ready.clear()
                    self.logger.debug(f"{self}: forward processing inputs")

                    try:
                        output = self.step(self.in_bound_data)
                        break
                    except Exception as e:
                        traceback_info = traceback.format_exc()
                        self.logger.error(traceback_info)
                        return None

        # If output generated, send it!
        if output:

            # If output is not DataChunk, just add as default
            if not isinstance(output, DataChunk):
                output_data_chunk = DataChunk()
                output_data_chunk.add("default", output)
            else:
                output_data_chunk = output

            # And then save the latest value
            self.latest_value = output_data_chunk

            # First, check that it is a node with outbound!
            if self.publisher:

                self.logger.debug(f"{self}: got outputs to publish!")

                # Add timestamp and step id to the DataChunk
                meta = output_data_chunk.get("meta")
                meta["value"]["ownership"].append(
                    {"name": self.state.name, "timestamp": datetime.datetime.now()}
                )
                output_data_chunk.update("meta", meta)

                # Send out the output to the OutputsHandler
                self.publisher.publish(output_data_chunk)
                self.logger.debug(f"{self}: published!")

            else:

                self.logger.warning(f"{self}: Do not have publisher yet given outputs")

        # Update the counter
        self.step_id += 1

    def step(self, data_chunks: Dict[str, DataChunk] = {}) -> Union[DataChunk, Any]:
        """User-define method.

        In this method, the logic that is executed within the ``Node``'s
        while loop. For data sources (no inputs), the ``step`` method
        will execute as fast as possible; therefore, it is important to
        add ``time.sleep`` to specify the sampling rate.

        For a ``Node`` that have inputs, these will be executed when new
        data is received.

        Args:
            data_chunks (Optional[Dict[str, DataChunk]]): For source nodes, this \
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

        # Shutting down publisher
        if self.publisher:
            self.publisher.shutdown()
            self.logger.debug(f"{self}: publisher shutdown")

        # Stop poller
        if self.poll_inputs_thread:
            self.poll_inputs_thread.join()
            self.logger.debug(f"{self}: polling thread shutdown")

        # Shutting down subscriber
        for sub in self.p2p_subs.values():
            sub.shutdown()
            self.logger.debug(f"{self}: subscriber shutdown")

        # Shutdown the inputs and outputs threads
        self.save_handler.shutdown()
        self.save_handler.join()
        self.state.finished = True
        self.logger.debug(f"{self}: save handler shutdown")

        # Shutting down networking
        if self.networking:

            # Inform the worker that the Node has finished its saving of data
            self.client.send(signal=NODE_MESSAGE.STATUS, data=self.state.to_dict())

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
        self.logger.setLevel(self.logging_level)
        self.logger.debug(f"{self}: initialized with context -> {self._context}")

        # Performing preparation for the while loop
        self._prep()

        # User-defined, possible error
        try:
            self.prep()
        except Exception as e:
            traceback_info = traceback.format_exc()
            self.logger.error(traceback_info)
            return None

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
