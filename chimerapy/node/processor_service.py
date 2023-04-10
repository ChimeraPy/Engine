from typing import Dict, List, Optional
import threading
import traceback
import datetime

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
        self.safe_exec(self.node.setup)

    def main(self):

        # Then start a thread to read the sub poller
        self.while_loop_thread = threading.Thread(target=self.while_loop)
        self.while_loop_thread.start()

    def teardown(self):

        if self.while_loop_thread:
            self.while_loop_thread.join()

        self.safe_exec(self.node.teardown)

        self.node.logger.debug(f"{self.node}-ProcessorService shutdown")

    ####################################################################
    ## Helper Methods
    ####################################################################

    def safe_exec(self, func, args: List = [], kwargs: Dict = {}):

        # Default value
        output = None

        try:
            output = func(*args, **kwargs)
        except Exception as e:
            traceback_info = traceback.format_exc()
            self.node.logger.error(traceback_info)

        return output

    def while_loop(self):

        while self.node.running:
            self.forward()

    def forward(self):

        # Default value
        output = None

        # If incoming data, feed it into the step
        if "poller" in self.node.services:
            while self.node.running:
                if self.node.services["poller"].inputs_ready.wait(timeout=1):

                    # Once we get them, pass them through!
                    self.node.services["poller"].inputs_ready.clear()
                    output = self.safe_exec(
                        self.node.step,
                        args=(self.node.services["poller"].in_bound_data),
                    )
                    break

        else:
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

            else:

                self.node.logger.warning(
                    f"{self}: Do not have publisher yet given outputs"
                )

        # Update the counter
        self.step_id += 1
