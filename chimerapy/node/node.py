from typing import Dict, Any, Optional, Union
import pathlib
import logging
import uuid
import datetime
import time
import tempfile

# Third-party Imports
import multiprocess as mp
import numpy as np
import pandas as pd

# Internal Imports
from .. import _logger
from ..states import NodeState
from ..networking import DataChunk
from ..service import ServiceGroup

# Service Imports
from .record_service import RecordService
from .processor_service import ProcessorService


class Node:

    services: ServiceGroup

    def __init__(
        self,
        name: str,
        debug_port: Optional[int] = None,
        logdir: Optional[Union[str, pathlib.Path]] = None,
    ):
        """Create a basic unit of computation in ChimeraPy.

        A node has three main functions that can be overwritten to add
        desired behavior: ``setup``, ``step``, and ``teardown``. You don't
        require them all if not necessary. The ``step`` function is executed
        within a while loop, when new inputs are available (if inputs are
        specified in the graph).

        If the ``step`` function is too restrictive, the ``main``
        (containing the while loop) can be overwritten instead.

        Args:
            name (str): The name that will later used to refer to the Node.

        """

        # Saving input parameters
        self.state = NodeState(id=str(uuid.uuid4()), name=name)
        self.debug_port = debug_port
        self._running: Union[bool, mp.Value] = True
        self.blocking = True

        # Generic Node needs
        self.logger: logging.Logger = logging.getLogger("chimerapy-node")
        self.logging_level: int = logging.DEBUG
        self.start_time = datetime.datetime.now()

        if logdir:
            self.logdir = str(logdir)
        else:
            self.logdir = str(tempfile.mkdtemp())
        self.logger.debug(f"{self}: logdir located in {self.logdir}")

        # Saving state variables
        self.services = ServiceGroup()

        # Services to be established
        for s_name, s_cls in {
            "record": RecordService,
            "processor": ProcessorService,
        }.items():
            s = s_cls(name=s_name)
            s.inject(self)

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
    def running(self) -> bool:
        if isinstance(self._running, bool):
            return self._running
        else:  # Shared multiprocessing variable
            return self._running.value

    @running.setter
    def running(self, value: bool):
        if isinstance(self._running, bool):
            self._running = value
        else:  # Shared multiprocessing variable
            self._running.value = value

    ####################################################################
    ## Utils
    ####################################################################

    def __repr__(self):
        return f"<Node name={self.state.name} id={self.state.id}>"

    def __str__(self):
        return self.__repr__()

    def get_logger(self) -> logging.Logger:

        # Get Logger
        logger = _logger.getLogger("chimerapy-node")
        logger.setLevel(self.logging_level)

        # If worker, add zmq handler
        if "worker" in self.services or self.debug_port:

            if "worker" in self.services:
                logging_port = self.services["worker"].worker_logging_port
            elif self.debug_port:
                logging_port = self.debug_port
            else:
                logging_port = 5555

            _logger.add_node_id_zmq_push_handler(
                logger, "127.0.0.1", logging_port, self.id
            )
        else:
            _logger.add_console_handler(logger)

        return logger

    ####################################################################
    ## Saving Data Stream API
    ####################################################################

    def save_video(self, name: str, data: np.ndarray, fps: int):

        if self.services["record"].enabled:
            timestamp = datetime.datetime.now()
            video_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "video",
                "fps": fps,
                "elapsed": (timestamp - self.start_time).total_seconds(),
                "timestamp": timestamp,
            }
            self.services["record"].submit(video_entry)

    def save_audio(
        self, name: str, data: np.ndarray, channels: int, format: int, rate: int
    ):

        if self.services["record"].enabled:
            audio_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "audio",
                "channels": channels,
                "format": format,
                "rate": rate,
                "timestamp": datetime.datetime.now(),
            }
            self.services["record"].submit(audio_entry)

    def save_tabular(
        self, name: str, data: Union[pd.DataFrame, Dict[str, Any], pd.Series]
    ):
        if self.services["record"].enabled:
            tabular_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "tabular",
                "timestamp": datetime.datetime.now(),
            }
            self.services["record"].submit(tabular_entry)

    def save_image(self, name: str, data: np.ndarray):

        if self.services["record"].enabled:
            image_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "image",
                "timestamp": datetime.datetime.now(),
            }
            self.services["record"].submit(image_entry)

    ####################################################################
    ## Back-End Lifecycle API
    ####################################################################

    def _setup(self):
        self.state.fsm = "INITIALIZED"
        self.services.apply(
            "setup", order=["record", "worker", "publisher", "poller", "processor"]
        )
        self.logger.debug(f"{self}: finished setup")

    def _ready(self):
        self.state.fsm = "READY"
        self.services.apply(
            "ready", order=["record", "publisher", "poller", "processor", "worker"]
        )
        self.logger.debug(f"{self}: is ready")

    def _wait(self):
        self.services.apply(
            "wait", order=["record", "publisher", "poller", "processor", "worker"]
        )
        self.logger.debug(f"{self}: finished waiting")

    def _main(self):
        # self.state.fsm = "PREVIEWING"
        self.services.apply(
            "main", order=["record", "publisher", "poller", "processor", "worker"]
        )
        self.logger.debug(f"{self}: finished main")

    def _idle(self):

        self.logger.debug(f"{self}: idle")
        while self.running:
            time.sleep(1)
        self.logger.debug(f"{self}: exiting idle")

    def _teardown(self):
        self.services.apply(
            "teardown", order=["record", "publisher", "poller", "processor", "worker"]
        )
        self.state.fsm = "SHUTDOWN"
        self.logger.debug(f"{self}: finished teardown")

    ####################################################################
    ## Front-facing Node Lifecycle API
    ####################################################################

    def setup(self):
        """User-defined method for ``Node`` setup.

        In this method, the setup logic of the ``Node`` is executed. This
        would include opening files, creating connections to sensors, and
        calibrating sensors.

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
        ...

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

    def teardown(self):
        """User-define method.

        This method provides a convienient way to shutdown services, such
        as closing files, signaling to sensors to stop, and making any
        last minute corrections to the data.

        """
        ...

    def run(self, blocking: bool = True, running: Optional[mp.Value] = None):
        """The actual method that is executed in the new process.

        When working with ``multiprocessing.Process``, it should be
        considered that the creation of a new process can yield
        unexpected behavior if not carefull. It is recommend that one
        reads the ``mutliprocessing`` documentation to understand the
        implications.

        """
        self.logger = self.get_logger()
        self.logger.setLevel(self.logging_level)

        # Saving synchronized variable
        if type(running) != type(None):
            self._running = running

        # Saving configuration
        self.blocking = blocking

        # Performing setup for the while loop
        self._setup()
        self._ready()
        self._wait()
        self._main()

        if self.blocking:
            self._idle()
            self._teardown()
            self.logger.debug(f"{self}: is exiting")
        else:
            self.state.fsm = "RUNNING"

    def shutdown(self, msg: Dict = {}):

        self.running = False
        self.logger.debug(f"{self}: shutting down")

        if not self.blocking:
            self._teardown()
            self.logger.debug(f"{self}: is exiting")
