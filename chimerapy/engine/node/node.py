import pathlib
import logging
import uuid
import datetime
import os
import asyncio
from concurrent.futures import Future
from typing import Dict, Any, Optional, Union, Literal, Coroutine, List

# Third-party Imports
import multiprocess as mp
import numpy as np
import pandas as pd

# Internal Imports
from chimerapy.engine import _logger
from ..states import NodeState
from ..networking import DataChunk
from ..networking.async_loop_thread import AsyncLoopThread
from ..eventbus import EventBus, Event

# Service Imports
from .worker_comms_service import WorkerCommsService
from .registered_method import RegisteredMethod
from .record_service import RecordService
from .processor_service import ProcessorService
from .poller_service import PollerService
from .publisher_service import PublisherService


class Node:

    worker_comms: Optional[WorkerCommsService]
    processor: Optional[ProcessorService]
    recorder: Optional[RecordService]
    poller: Optional[PollerService]
    publisher: Optional[PublisherService]

    registered_methods: Dict[str, RegisteredMethod] = {}
    context: Literal["main", "multiprocessing", "threading"]

    def __init__(
        self,
        name: str,
        debug_port: Optional[int] = None,
        logdir: Optional[Union[str, pathlib.Path]] = None,
        id: Optional[str] = None,
    ):
        """Create a basic unit of computation in ChimeraPy-Engine.

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
        # Handle optional parameters
        kwargs = {"name": name, "registered_methods": self.registered_methods}
        if id:
            kwargs["id"] = id
        if logdir:
            kwargs["logdir"] = logdir

        # Saving input parameters
        self.state = NodeState(**kwargs)
        self.debug_port = debug_port

        # State variables
        self._running: Union[bool, mp.Value] = True
        self.blocking = True
        self.task_futures: List[Future] = []

        # Generic Node needs
        self.logger: logging.Logger = logging.getLogger("chimerapy-engine-node")
        self.logging_level: int = logging.DEBUG
        self.start_time = datetime.datetime.now()

        # Default values
        self.node_config = None
        self.worker_comms = None
        self.processor = None
        self.recorder = None
        self.poller = None
        self.publisher = None

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
        logger = _logger.getLogger("chimerapy-engine-node")
        logger.setLevel(self.logging_level)

        # Do not add handler in threaded mode
        if self.context != "multiprocessing":
            return logger

        # If worker, add zmq handler
        if self.worker_comms or self.debug_port:

            if self.worker_comms:
                logging_port = self.worker_comms.worker_logging_port
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

    def add_worker_service(self, worker_service: WorkerCommsService):

        # Store service
        self.worker_service = worker_service

        # Add the context information
        self.config = worker_service.node_config

        # Creating logdir after given the Node
        self.state.logdir = str(worker_service.worker_logdir / self.state.name)
        os.makedirs(self.state.logdir, exist_ok=True)

    def _exec_coro(self, coro: Coroutine) -> Future:
        # Submitting the coroutine
        future = self._thread.exec(coro)

        # Saving the future for later use
        self.task_futures.append(future)

        return future

    ####################################################################
    ## Saving Data Stream API
    ####################################################################

    def save_video(self, name: str, data: np.ndarray, fps: int):

        if self.recorder.enabled:
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
            self.recorder.submit(video_entry)

    def save_audio(
        self, name: str, data: np.ndarray, channels: int, format: int, rate: int
    ):
        """Record audio data.

        Parameters
        ----------
        name : str
            Name of the audio data (.wav extension will be suffixed).
        data : np.ndarray
            Audio data as a numpy array.
        channels : int
            Number of channels.
        format : int
            Format of the audio data.
        rate : int
            Sampling rate of the audio data.

        Notes
        -----
        It is the implementation's responsibility to properly format the data

        """
        if self.recorder.enabled:
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
            self.recorder.submit(audio_entry)

    def save_tabular(
        self, name: str, data: Union[pd.DataFrame, Dict[str, Any], pd.Series]
    ):
        if self.recorder.enabled:
            tabular_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "tabular",
                "timestamp": datetime.datetime.now(),
            }
            self.recorder.submit(tabular_entry)

    def save_image(self, name: str, data: np.ndarray):

        if self.recorder.enabled:
            image_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "image",
                "timestamp": datetime.datetime.now(),
            }
            self.recorder.submit(image_entry)

    ####################################################################
    ## Back-End Lifecycle API
    ####################################################################

    async def _eventloop(self):
        await self._setup()
        # await self._ready()
        # await self._wait()
        # await self._main()
        await self._idle() # stop, running, and collecting
        await self._teardown()

    async def _setup(self):
        self.state.fsm = "INITIALIZED"
        await self.eventbus.asend(Event("setup"))
        # self.logger.debug(f"{self}: finished setup")

    # async def _ready(self):
    #     self.state.fsm = "READY"
    #     await self.eventbus.asend(Event("ready"))
        # self.logger.debug(f"{self}: is ready")

    # async def _wait(self):
    #     await self.eventbus.asend(Event("wait"))
    #     # self.logger.debug(f"{self}: finished waiting")

    # async def _main(self):
    #     # self.state.fsm = "PREVIEWING"
    #     await self.eventbus.asend(Event("main"))
    #     # self.logger.debug(f"{self}: finished main")

    async def _idle(self):

        # self.logger.debug(f"{self}: idle")
        while self.running:
            await asyncio.sleep(1)
        # self.logger.debug(f"{self}: exiting idle")

    async def _teardown(self):
        await self.eventbus.asend("teardown")
        self.state.fsm = "SHUTDOWN"
        # self.logger.debug(f"{self}: finished teardown")

    ####################################################################
    ## Front-facing Node Lifecycle API
    ####################################################################

    def setup(self):
        """User-defined method for ``Node`` setup.

        In this method, the setup logic of the ``Node`` is executed. This
        would include opening files, creating connections to sensors, and
        calibrating sensors.

        Can be overwritten with both sync and async setup functions.

        """
        ...

    def main(self):
        """User-possible overwritten method.

        This method can also be overwritten, through it is recommend to
        do so carefully. If overwritten, the handling of inputs will have
        to be implemented as well.

        To receive the data, use the EventBus to listen to the 'in_step'
        event and emit the output data with 'out_step'.

        Can be overwritten with both sync and async main functions.

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

        Can be overwritten with both sync and async step functions.

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

        Can be overwritten with both sync and async teardown functions.

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

        # Create the services
        self._thread = AsyncLoopThread()
        self._thread.start()

        # Create the event bus for the Worker
        self.eventbus = EventBus(thread=self._thread)

        # Adding state to the WorkerCommsService
        if self.worker_comms:
            self.worker_comms.add_state(self.state)

        # Configure the processor
        # if self.__class__.__bases__[0].main.__code__ != self.main.__code__:
        #     ...

        # Create services
        self.processor = ProcessorService(
            name="processor",
            state=self.state,
            eventbus=self.eventbus,
            in_bound_data=len(self.node_config.in_bound) != 0,
            logger=self.logger,
        )
        self.recorder = RecordService(
            name="recorder",
            state=self.state,
            eventbus=self.eventbus,
            logger=self.logger,
        )

        # If in-bound, enable the poller service
        if self.node_config and self.node_config.in_bound:
            self.poller = PollerService(
                name="poller",
                in_bound=self.node_config.in_bound,
                in_bound_by_name=self.node_config.in_bound_by_name,
                follow=self.node_config.follow,
                state=self.state,
                eventbus=self.eventbus,
                logger=self.logger,
            )

        # If out_bound, enable the publisher service
        if self.node_config and self.node_config.out_bound:
            self.publisher = PublisherService(
                "publisher",
                state=self.state,
                eventbus=self.eventbus,
                logger=self.logger,
            )

        # Performing setup for the while loop
        future = self._exec_coro(self._eventloop)

        if self.blocking:
            future.result()

        return future
