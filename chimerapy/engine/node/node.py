import asyncio
import datetime
import logging
import os
import pathlib
import tempfile
import uuid
from asyncio import Task
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Dict, List, Literal, Optional, Union

# Third-party Imports
import multiprocess as mp
import numpy as np
import pandas as pd
from aiodistbus import EntryPoint, EventBus, make_evented

# Internal Imports
from chimerapy.engine import _logger

from ..networking import DataChunk
from ..service import Service
from ..states import NodeState
from ..utils import run_coroutine_in_thread
from .fsm_service import FSMService

# Service Imports
from .node_config import NodeConfig
from .poller_service import PollerService
from .processor_service import ProcessorService
from .profiler_service import ProfilerService
from .publisher_service import PublisherService
from .record_service import RecordService
from .registered_method import RegisteredMethod
from .struct import PreSetupData
from .worker_comms_service import WorkerCommsService


class Node:
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
        self.executor: Optional[ThreadPoolExecutor] = None
        self.eventloop_future: Optional[Future] = None
        self.eventloop_task: Optional[Task] = None
        self._save_tasks: List[Task] = []
        self.task_futures: List[Future] = []
        self.services: List[Service] = []
        self.bus: Optional[EventBus] = None
        self.entrypoint = EntryPoint()

        # Generic Node needs
        self.logger: logging.Logger = logging.getLogger("chimerapy-engine-node")
        self.logging_level: int = logging.DEBUG

        # Default values
        self.node_config = NodeConfig()

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
        if "WorkerCommsService" in self.services and isinstance(
            self.services["WorkerCommsService"], WorkerCommsService
        ):
            worker_comms = self.services["WorkerCommsService"]
        else:
            worker_comms = None

        if worker_comms or self.debug_port:

            if worker_comms:
                logging_port = worker_comms.worker_logging_port
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

        # Add the context information
        self.node_config = worker_comms.node_config

        # Creating logdir after given the Node
        self.state.logdir = worker_comms.worker_logdir / self.state.name
        os.makedirs(self.state.logdir, exist_ok=True)

        # Store service
        self.services.append(worker_comms)

    ####################################################################
    ## Saving Data Stream API
    ####################################################################

    def save_video(self, name: str, data: np.ndarray, fps: int):

        timestamp = datetime.datetime.now()
        video_entry = {
            "uuid": uuid.uuid4(),
            "name": name,
            "data": data,
            "dtype": "video",
            "fps": fps,
            "timestamp": timestamp,
        }
        self._save_tasks.append(
            self.loop.create_task(self.entrypoint.emit("record_entry", video_entry))
        )

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
        self._save_tasks.append(
            self.loop.create_task(self.entrypoint.emit("record_entry", audio_entry))
        )

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
        self._save_tasks.append(
            self.loop.create_task(self.entrypoint.emit("record_entry", audio_entry))
        )

    def save_tabular(
        self, name: str, data: Union[pd.DataFrame, Dict[str, Any], pd.Series]
    ):
        tabular_entry = {
            "uuid": uuid.uuid4(),
            "name": name,
            "data": data,
            "dtype": "tabular",
            "timestamp": datetime.datetime.now(),
        }
        self._save_tasks.append(
            self.loop.create_task(self.entrypoint.emit("record_entry", tabular_entry))
        )

    def save_image(self, name: str, data: np.ndarray):
        image_entry = {
            "uuid": uuid.uuid4(),
            "name": name,
            "data": data,
            "dtype": "image",
            "timestamp": datetime.datetime.now(),
        }
        self._save_tasks.append(
            self.loop.create_task(self.entrypoint.emit("record_entry", image_entry))
        )

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
        json_entry = {
            "uuid": uuid.uuid4(),
            "name": name,
            "data": data,
            "dtype": "json",
            "timestamp": datetime.datetime.now(),
        }
        self._save_tasks.append(
            self.loop.create_task(self.entrypoint.emit("record_entry", json_entry))
        )

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

        text_entry = {
            "uuid": uuid.uuid4(),
            "name": name,
            "data": data,
            "suffix": suffix,
            "dtype": "text",
            "timestamp": datetime.datetime.now(),
        }
        self._save_tasks.append(
            self.loop.create_task(self.entrypoint.emit("record_entry", text_entry))
        )

    ####################################################################
    ## Back-End Lifecycle API
    ####################################################################

    async def _setup(self):

        # Make the state evented
        self.state = make_evented(self.state, bus=self.bus)

        # Add the FSM service
        self.services.append(FSMService("fsm", self.state, self.logger))

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
        self.services.append(
            ProcessorService(
                name="processor",
                setup_fn=self.setup,
                main_fn=main_fn,
                teardown_fn=self.teardown,
                operation_mode=mode,
                registered_methods=self.registered_methods,
                registered_node_fns=registered_fns,
                state=self.state,
                in_bound_data=in_bound_data,
                logger=self.logger,
            )
        )
        self.services.append(
            RecordService(
                name="recorder",
                state=self.state,
                logger=self.logger,
            )
        )
        self.services.append(
            ProfilerService(
                name="profiler",
                state=self.state,
                logger=self.logger,
            )
        )

        # If in-bound, enable the poller service
        if self.node_config and self.node_config.in_bound:
            self.services.append(
                PollerService(
                    name="poller",
                    in_bound=self.node_config.in_bound,
                    in_bound_by_name=self.node_config.in_bound_by_name,
                    follow=self.node_config.follow,
                    state=self.state,
                    logger=self.logger,
                )
            )

        # If out_bound, enable the publisher service
        if self.node_config and self.node_config.out_bound:
            self.services.append(
                PublisherService(
                    "publisher",
                    state=self.state,
                    logger=self.logger,
                )
            )

        # Initialize all services
        for s in self.services:
            await s.attach(self.bus)

        # Presetup
        data = PreSetupData(self.state, self.logger)
        await self.entrypoint.emit("pre_setup", data)

        # Create the directory
        if not self.state.logdir:
            self.state.logdir = pathlib.Path(tempfile.mktemp())
        os.makedirs(self.state.logdir, exist_ok=True)

        # Start all services
        await self.entrypoint.emit("setup")
        # self.logger.debug(f"{self}: setup complete")

    async def _eventloop(self):
        # self.logger.debug(f"{self}: within event loop")
        await self._idle()  # stop, running, and collecting
        # self.logger.debug(f"{self}: after idle")
        await self._teardown()
        # self.logger.debug(f"{self}: exiting")
        return 1

    async def _idle(self):
        while self.running:
            await asyncio.sleep(0.2)

    async def _teardown(self):
        await asyncio.gather(*self._save_tasks)
        await self.entrypoint.emit("teardown")

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
        bus: Optional[EventBus] = None,
        running: Optional[mp.Value] = None,  # type: ignore
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

        # Have to run setup before letting the system continue
        run_func = lambda x: run_coroutine_in_thread(self.arun(x))
        self.executor = ThreadPoolExecutor(max_workers=1)
        self.eventloop_future = self.executor.submit(run_func, bus)
        return self.eventloop_future.result()

    async def arun(self, bus: Optional[EventBus] = None):
        self.logger = self.get_logger()
        self.logger.setLevel(self.logging_level)
        self.loop = asyncio.get_event_loop()

        # Save parameters
        if bus:
            self.bus = bus
        else:
            self.bus = EventBus()

        # Create an entrypoint
        await self.entrypoint.connect(self.bus)

        await self._setup()
        return await self._eventloop()

    def shutdown(self, timeout: Optional[Union[float, int]] = None):
        self.running = False
        if self.eventloop_future:
            if timeout:
                self.eventloop_future.result(timeout=timeout)
            else:
                self.eventloop_future.result()

    async def ashutdown(self):
        self.running = False
        if self.eventloop_task:
            await self.eventloop_task
