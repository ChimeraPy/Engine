import asyncio
import datetime
import logging
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Coroutine, Dict, List, Literal, Optional

from aiodistbus import registry

from chimerapy.engine import _logger

from ..data_protocols import (
    GatherData,
    RegisteredMethod,
    RegisteredMethodData,
    ResultsData,
)
from ..networking import DataChunk
from ..service import Service
from ..states import NodeState


class ProcessorService(Service):
    def __init__(
        self,
        name: str,
        state: NodeState,
        in_bound_data: bool,
        setup_fn: Optional[Callable] = None,
        main_fn: Optional[Callable] = None,
        teardown_fn: Optional[Callable] = None,
        registered_methods: Dict[str, RegisteredMethod] = {},
        registered_node_fns: Dict[str, Callable] = {},
        operation_mode: Literal["main", "step"] = "step",
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name)

        # Saving input parameters
        self.state = state
        self.setup_fn = setup_fn
        self.main_fn = main_fn
        self.teardown_fn = teardown_fn
        self.operation_mode = operation_mode
        self.in_bound_data = in_bound_data
        self.registered_methods = registered_methods
        self.registered_node_fns = registered_node_fns

        if logger:
            self.logger = logger
        else:
            self.logger = _logger.getLogger("chimerapy-engine")

        # Containers
        self.latest_data_chunk = DataChunk()
        self.step_id: int = 0
        self.running: bool = False
        self.running_task: Optional[asyncio.Task] = None
        self.tasks: List[asyncio.Task] = []
        self.main_thread: Optional[threading.Thread] = None
        self.executor: ThreadPoolExecutor = ThreadPoolExecutor()

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    @registry.on("setup", namespace=f"{__name__}.ProcessorService")
    async def setup(self):

        # Create threading information
        self.step_lock = threading.Lock()

        # Executing setup
        if self.setup_fn:
            await self.safe_exec(self.setup_fn)

    @registry.on("start", namespace=f"{__name__}.ProcessorService")
    async def start(self):
        # Create a task
        self.running_task = asyncio.create_task(self.main())

    async def main(self):

        # Set the flag
        self.running: bool = True

        # Only if method is provided
        if self.main_fn:

            # Handling different operational modes
            if self.operation_mode == "main":
                # self.logger.debug(f"{self}: operational mode = main")

                if asyncio.iscoroutinefunction(self.main_fn):
                    await self.safe_exec(self.main_fn)
                else:
                    self.main_thread = threading.Thread(target=self.main_fn)
                    self.main_thread.start()

            elif self.operation_mode == "step":

                # If step or sink node, only run with inputs
                if self.in_bound_data:
                    # self.logger.debug(f"{self}: step node: {self.state.id}")
                    await self.entrypoint.on(
                        "in_step", self.safe_step, Dict[str, DataChunk]
                    )

                # If source, run as fast as possible
                else:
                    # self.logger.debug(f"{self}: source node: {self.state.id}")
                    while self.running:
                        await self.safe_step()

    @registry.on("stop", namespace=f"{__name__}.ProcessorService")
    async def stop(self):
        self.running = False

    @registry.on("teardown", namespace=f"{__name__}.ProcessorService")
    async def teardown(self):

        # Stop things
        self.running = False

        # If main thread, stop
        if self.main_thread:
            self.main_thread.join()

        # Shutting down running task
        if self.running_task:
            await self.running_task

        if self.teardown_fn:
            await self.safe_exec(self.teardown_fn)

    ####################################################################
    ## Debugging tools
    ####################################################################

    @registry.on("gather", namespace=f"{__name__}.ProcessorService")
    async def gather(self):
        data = GatherData(node_id=self.state.id, output=self.latest_data_chunk)
        await self.entrypoint.emit("gather_results", data)

    ####################################################################
    ## Async Registered Methods
    ####################################################################

    @registry.on(
        "registered_method",
        RegisteredMethodData,
        namespace=f"{__name__}.ProcessorService",
    )
    async def execute_registered_method(
        self, reg_method_req: RegisteredMethodData
    ) -> ResultsData:

        # First check if the request is valid
        if reg_method_req.method_name not in self.registered_methods:
            self.logger.warning(
                f"{self}: Worker requested execution of registered method that doesn't "
                f"exists: {reg_method_req.method_name}"
            )
            data = ResultsData(node_id=self.state.id, success=False, output=None)
            return data

        # Extract the method
        function: Callable[[], Coroutine] = self.registered_node_fns[
            reg_method_req.method_name
        ]
        style = self.registered_methods[reg_method_req.method_name].style
        self.logger.debug(
            f"{self}: executing {function} with params: {reg_method_req.params}"
        )

        # Execute method based on its style
        success = False
        if style == "concurrent":
            # output = await function(**params)  # type: ignore[call-arg]
            output, _ = await self.safe_exec(function, kwargs=reg_method_req.params)
            success = True

        elif style == "blocking":
            with self.step_lock:
                output, _ = await self.safe_exec(function, kwargs=reg_method_req.params)
                success = True

        elif style == "reset":
            with self.step_lock:
                output, _ = await self.safe_exec(function, kwargs=reg_method_req.params)
                await self.entrypoint.emit("reset")
                success = True

        else:
            self.logger.error(f"Invalid registered method request: style={style}")
            output = None

        data = ResultsData(node_id=self.state.id, success=success, output=output)
        await self.entrypoint.emit("registered_method_results", data)
        return data

    ####################################################################
    ## Helper Methods
    ####################################################################

    async def safe_exec(
        self, func: Callable, args: List = [], kwargs: Dict = {}
    ) -> Any:

        # Default value
        output = None
        tic = time.perf_counter()

        try:
            if asyncio.iscoroutinefunction(func):
                output = await func(*args, **kwargs)
            else:
                output = await asyncio.get_running_loop().run_in_executor(
                    self.executor, lambda: func(*args, **kwargs)
                )
        except Exception:
            traceback_info = traceback.format_exc()
            self.logger.error(traceback_info)

        # Compute delta
        toc = time.perf_counter()
        delta = (toc - tic) * 1000

        return output, delta

    async def safe_step(self, data_chunks: Dict[str, DataChunk] = {}):

        # Default value
        output = None
        delta = 0

        if self.main_fn:  # If user-defined ``step``
            with self.step_lock:
                if self.in_bound_data:
                    output, delta = await self.safe_exec(
                        self.main_fn, kwargs={"data_chunks": data_chunks}
                    )
                else:
                    output, delta = await self.safe_exec(self.main_fn)

        # If output generated, send it!
        if output:

            # If output is not DataChunk, just add as default
            if not isinstance(output, DataChunk):
                output_data_chunk = DataChunk()
                output_data_chunk.add("default", output)
            else:
                output_data_chunk = output

            # And then save the latest value
            self.latest_data_chunk = output_data_chunk

            # Add timestamp and step id to the DataChunk
            meta = output_data_chunk.get("meta")
            meta["value"]["transmitted"] = datetime.datetime.now()
            meta["value"]["delta"] = delta
            output_data_chunk.update("meta", meta)

            # Send out the output to the OutputsHandler
            await self.entrypoint.emit("out_step", output_data_chunk)
            # self.logger.debug(f"{self}: output = {output_data_chunk}")

        # Update the counter
        self.step_id += 1
