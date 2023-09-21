import pathlib
import logging
import uuid
import datetime
import os
import tempfile
import asyncio
from concurrent.futures import Future
from typing import Dict, Any, Optional, Union, Literal, Coroutine, List

# Third-party Imports
import multiprocess as mp
import numpy as np
import pandas as pd

# Internal Imports
from chimerapy.engine import _logger
from chimerapy.engine import config
from ..states import NodeState
from ..networking import DataChunk
from ..networking.async_loop_thread import AsyncLoopThread
from ..eventbus import EventBus, Event, make_evented

# Service Imports
from .node_config import NodeConfig
from .registered_method import RegisteredMethod
from .profiler_service import ProfilerService
from .fsm_service import FSMService
from .worker_comms_service import WorkerCommsService
from .record_service import RecordService
from .processor_service import ProcessorService
from .poller_service import PollerService
from .publisher_service import PublisherService


class Node:

    # worker_comms: Optional[WorkerCommsService]
    # processor: Optional[ProcessorService]
    # recorder: Optional[RecordService]
    # poller: Optional[PollerService]
    # publisher: Optional[PublisherService]

    # registered_methods: Dict[str, RegisteredMethod] = {}
    # context: Literal["main", "multiprocessing", "threading"]

    def __init__(
        self,
        name: str,
        debug_port: Optional[int] = None,
        logdir: Optional[pathlib.Path] = None,
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
        if not id:
            id = str(uuid.uuid4())

        # Handle registered methods
        if not hasattr(self, "registered_methods"):
            self.registered_methods: Dict[str, RegisteredMethod] = {}

        # Saving input parameters
        self.state = NodeState(
            name=name, id=id, registered_methods=self.registered_methods, logdir=logdir
        )
        self.debug_port = debug_port

        # State variables
        self._running: Union[bool, mp.Value] = True  # type: ignore
        self.eventloop_future: Optional[Future] = None
        self.blocking = True
        self.task_futures: List[Future] = []

        # Generic Node needs
        self.logger: logging.Logger = logging.getLogger("chimerapy-engine-node")
        self.logging_level: int = logging.DEBUG

        # Default values
        self.node_config = NodeConfig()
        self.worker_comms: Optional[WorkerCommsService] = None
        self.processor: Optional[ProcessorService] = None
        self.recorder: Optional[RecordService] = None
        self.poller: Optional[PollerService] = None
        self.publisher: Optional[PublisherService] = None

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
        if self.node_config.context != "multiprocessing":
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

    def add_worker_comms(self, worker_comms: WorkerCommsService):

        # Store service
        self.worker_comms = worker_comms
        self.worker_comms

        # Add the context information
        self.node_config = worker_comms.node_config

        # Creating logdir after given the Node
        self.state.logdir = worker_comms.worker_logdir / self.state.name
        os.makedirs(self.state.logdir, exist_ok=True)

    def _exec_coro(self, coro: Coroutine) -> Future:
        assert self._thread
        # Submitting the coroutine
        future = self._thread.exec(coro)

        # Saving the future for later use
        self.task_futures.append(future)

        return future

    ####################################################################
    ## Saving Data Stream API
    ####################################################################

    def save_video(self, name: str, data: np.ndarray, fps: int):

        if not self.recorder:
            self.logger.warning(
                f"{self}: cannot perform recording operation without RecorderService "
                "initialization"
            )
            return False

        if self.recorder.enabled:
            timestamp = datetime.datetime.now()
            video_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "video",
                "fps": fps,
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
        if not self.recorder:
            self.logger.warning(
                f"{self}: cannot perform recording operation without RecorderService "
                "initialization"
            )
            return False

        if self.recorder.enabled:
            audio_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "audio",
                "channels": channels,
                "format": format,
                "rate": rate,
                "recorder_version": 1,
                "timestamp": datetime.datetime.now(),
            }
            self.recorder.submit(audio_entry)

    def save_audio_v2(
        self,
        name: str,
        data: bytes,
        channels: int,
        sampwidth: int,
        framerate: int,
        nframes: int,
    ) -> None:
        """Record audio data version 2.

        Parameters
        ----------
        name : str
            Name of the audio data (.wav extension will be suffixed).
        data : bytes
            Audio data as a bytes object.
        channels : int
            Number of channels.
        sampwidth : int
            Sample width in bytes.
        framerate : int
            Sampling rate of the audio data.
        nframes : int
            Number of frames.
        """
        if not self.recorder:
            self.logger.warning(
                f"{self}: cannot perform recording operation without RecorderService "
                "initialization"
            )
            return

        if self.recorder.enabled:
            audio_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "audio",
                "channels": channels,
                "sampwidth": sampwidth,
                "framerate": framerate,
                "nframes": nframes,
                "recorder_version": 2,
                "timestamp": datetime.datetime.now(),
            }
            self.recorder.submit(audio_entry)

    def save_tabular(
        self, name: str, data: Union[pd.DataFrame, Dict[str, Any], pd.Series]
    ):
        if not self.recorder:
            self.logger.warning(
                f"{self}: cannot perform recording operation without RecorderService "
                "initialization"
            )
            return False

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
        if not self.recorder:
            self.logger.warning(
                f"{self}: cannot perform recording operation without RecorderService "
                "initialization"
            )
            return False

        if self.recorder.enabled:
            image_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "image",
                "timestamp": datetime.datetime.now(),
            }
            self.recorder.submit(image_entry)

    def save_json(self, name: str, data: Dict[Any, Any]):
        """Record json data from the node to a JSON Lines file.

        Parameters
        ----------
        name : str
            Name of the json file (.jsonl extension will be suffixed).

        data : Dict[Any, Any]
            The data to be recorded.

        Notes
        -----
        The data is recorded in JSON Lines format, which is a sequence of JSON objects.
        The data dictionary provided must be JSON serializable.
        """

        if not self.recorder:
            self.logger.warning(
                f"{self}: cannot perform recording operation without RecorderService "
                "initialization"
            )
            return False

        if self.recorder.enabled:
            json_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "dtype": "json",
                "timestamp": datetime.datetime.now(),
            }
            self.recorder.submit(json_entry)

    def save_text(self, name: str, data: str, suffix="txt"):
        """Record text data from the node to a text file.

        Parameters
        ----------
        name : str
            Name of the text file (.suffix extension will be suffixed).

        data : str
            The data to be recorded.

        suffix : str
            The suffix of the text file.

        Notes
        -----
        It should be noted that new lines addition should be taken by the callee.
        """

        if not self.recorder:
            self.logger.warning(
                f"{self}: cannot perform recording operation without RecorderService "
                "initialization"
            )
            return False

        if self.recorder.enabled:
            text_entry = {
                "uuid": uuid.uuid4(),
                "name": name,
                "data": data,
                "suffix": suffix,
                "dtype": "text",
                "timestamp": datetime.datetime.now(),
            }
            self.recorder.submit(text_entry)

    ####################################################################
    ## Back-End Lifecycle API
    ####################################################################

    async def _eventloop(self):
        # self.logger.debug(f"{self}: within event loop")
        await self._idle()  # stop, running, and collecting
        await self._teardown()
        return True

    async def _setup(self):
        await self.eventbus.asend(Event("setup"))

    async def _idle(self):
        while self.running:
            await asyncio.sleep(1)
        # self.logger.debug(f"{self}: exiting idle")

    async def _teardown(self):
        await self.eventbus.asend(Event("teardown"))

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

    def run(
        self,
        blocking: bool = True,
        running: Optional[mp.Value] = None,  # type: ignore
        eventbus: Optional[EventBus] = None,
    ):
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

        # If given eventbus, use it
        if eventbus:
            self.eventbus = eventbus
            self._thread = eventbus.thread

        # Else, create the eventbus yourself
        else:
            self._thread = AsyncLoopThread()
            self._thread.start()
            self.eventbus = EventBus(thread=self._thread)

        # Adding state to the WorkerCommsService
        if self.worker_comms:
            self.worker_comms.in_node_config(
                state=self.state, eventbus=self.eventbus, logger=self.logger
            )
            if self.worker_comms.worker_config:
                config.update_defaults(self.worker_comms.worker_config)
        elif not self.state.logdir:
            self.state.logdir = pathlib.Path(tempfile.mktemp())

        # Create the directory
        if self.state.logdir:
            os.makedirs(self.state.logdir, exist_ok=True)
        else:
            raise RuntimeError(f"{self}: logdir {self.state.logdir} not set!")

        # Make the state evented
        self.state = make_evented(self.state, self.eventbus)

        # Add the FSM service
        self.fsm_service = FSMService("fsm", self.state, self.eventbus, self.logger)

        # Configure the processor's operational mode
        mode: Literal["main", "step"] = "step"  # default
        p_main = self.__class__.__bases__[0].main.__code__  # type: ignore[attr-defined]
        if p_main != self.main.__code__:
            # self.logger.debug(f"{self}: selected 'main' operational mode")
            main_fn = self.main
            mode = "main"
        else:
            # self.logger.debug(f"{self}: selected 'step' operational mode")
            main_fn = self.step
            mode = "step"

        # Obtain the registerd methods for the processor
        if self.registered_methods:
            registered_fns = {
                fname: getattr(self, fname) for fname in self.registered_methods
            }
        else:
            registered_fns = {}

        # Identity the type of Node (source, step, or sink)
        in_bound_data = len(self.node_config.in_bound) != 0

        # Create services
        self.processor = ProcessorService(
            name="processor",
            setup_fn=self.setup,
            main_fn=main_fn,
            teardown_fn=self.teardown,
            operation_mode=mode,
            registered_methods=self.registered_methods,
            registered_node_fns=registered_fns,
            state=self.state,
            eventbus=self.eventbus,
            in_bound_data=in_bound_data,
            logger=self.logger,
        )
        self.recorder = RecordService(
            name="recorder",
            state=self.state,
            eventbus=self.eventbus,
            logger=self.logger,
        )
        self.profiler = ProfilerService(
            name="profiler",
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

        # Have to run setup before letting the system continue
        self.eventbus.send(Event("initialize")).result()
        self._exec_coro(self._setup()).result()

        # Performing setup for the while loop
        self.eventloop_future = self._exec_coro(self._eventloop())

        if self.blocking:
            self.eventloop_future.result()

    def shutdown(self, timeout: Optional[Union[float, int]] = None):

        self.running = False

        if self.eventloop_future:
            if timeout:
                self.eventloop_future.result(timeout=timeout)
            else:
                self.eventloop_future.result()
