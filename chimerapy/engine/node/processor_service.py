from typing import Dict, List, Optional, Callable, Coroutine, Any
import threading
import traceback
import datetime
import time

from ..networking import DataChunk
from .node_service import NodeService


class ProcessorService(NodeService):
    def __init__(self, name: str):
        super().__init__(name)

        self.while_loop_thread: Optional[threading.Thread] = None
        self.latest_data_chunk = DataChunk()
        self.step_id: int = 0

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    def setup(self):
        # Create threading information
        self.step_lock = threading.Lock()
        self.reset_event = threading.Event()
        self.reset_event.set()

        # Executing setup
        self.safe_exec(self.node.setup)

    def main(self):

        # Then start a thread to read the sub poller
        self.while_loop_thread = threading.Thread(target=self.while_loop)
        self.while_loop_thread.start()

    def teardown(self):

        if self.while_loop_thread:
            self.while_loop_thread.join()

        self.safe_exec(self.node.teardown)

        # self.node.logger.debug(f"{self}: shutdown")

    ####################################################################
    ## Async Registered Methods
    ####################################################################

    async def execute_registered_method(
        self,
        method_name: str,
        params: Dict,
    ) -> Dict[str, Any]:

        # Extract the method
        function: Callable[[], Coroutine] = getattr(self.node, method_name)
        style = self.node.registered_methods[method_name].style
        self.node.logger.debug(f"{self}: executing {function} with params: {params}")

        # Execute method based on its style
        success = False
        if style == "concurrent":
            output = await function(**params)  # type: ignore[call-arg]
            success = True

        elif style == "blocking":
            with self.step_lock:
                output = await function(**params)  # type: ignore[call-arg]
                success = True

        elif style == "reset":
            with self.step_lock:
                output = await function(**params)  # type: ignore[call-arg]
                success = True

            # Signal the reset event and then wait
            if self.node.state.fsm in ["PREVIEWING", "RECORDING"]:
                self.reset_event.clear()
                success = self.reset_event.wait()

        else:
            self.node.logger.error(f"Invalid registered method request: style={style}")
            output = None

        return {"success": success, "output": output}

    ####################################################################
    ## Helper Methods
    ####################################################################

    def safe_exec(self, func, args: List = [], kwargs: Dict = {}):

        # Default value
        output = None

        try:
            output = func(*args, **kwargs)
        except Exception:
            traceback_info = traceback.format_exc()
            self.node.logger.error(traceback_info)

        return output

    def while_loop(self):

        # self.node.logger.debug(f"{self}: started processor while loop")

        # If using main method
        if self.node.__class__.__bases__[0].main.__code__ != self.node.main.__code__:
            # self.node.logger.debug(f"{self}: Using ``main`` method")
            self.safe_exec(self.node.main)

        # Else, step method
        else:
            # self.node.logger.debug(f"{self}: Using ``step`` method")

            while self.node.running:

                if self.node.state.fsm in ["PREVIEWING", "RECORDING"]:
                    self.forward()
                else:
                    time.sleep(0.1)

                # Allow the execution of user-defined registered methods
                if not self.reset_event.is_set():
                    self.safe_exec(self.node.teardown)
                    self.safe_exec(self.node.setup)
                    self.reset_event.set()

    def forward(self):

        # Default value
        output = None

        # If incoming data, feed it into the step
        if "poller" in self.node.services:
            while self.node.running:
                if self.node.services["poller"].inputs_ready.wait(timeout=1):

                    # Once we get them, pass them through!
                    self.node.services["poller"].inputs_ready.clear()
                    with self.step_lock:
                        output = self.safe_exec(
                            self.node.step,
                            args=[self.node.services["poller"].in_bound_data],
                        )
                    break

        else:
            with self.step_lock:
                output = self.safe_exec(self.node.step)

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

            # First, check that it is a node with outbound!
            if "publisher" in self.node.services:

                # Add timestamp and step id to the DataChunk
                meta = output_data_chunk.get("meta")
                meta["value"]["ownership"].append(
                    {"name": self.node.state.name, "timestamp": datetime.datetime.now()}
                )
                output_data_chunk.update("meta", meta)

                # Send out the output to the OutputsHandler
                self.node.services["publisher"].publish(output_data_chunk)
                # self.logger.debug(f"{self}: published!")

            # else:

            # self.node.logger.warning(
            #     f"{self}: Do not have publisher yet given outputs"
            # )

        # Update the counter
        self.step_id += 1
