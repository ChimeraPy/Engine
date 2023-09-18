import threading
import typing
import logging
from typing import Any, Union, Optional

# Third-party Imports
import multiprocess as mp

from chimerapy.engine import config
from ...networking import DataChunk

if typing.TYPE_CHECKING:
    from ...node.node import Node


class NodeController:
    node_object: "Node"

    context: Union[threading.Thread, mp.Process]  # type: ignore

    response: bool = False
    gather: DataChunk = DataChunk()
    registered_method_results: Any = None

    def __init__(self, node_object: "Node", logger: logging.Logger):

        # Save parameters
        self.node_object = node_object
        self.logger = logger

    def start(self):
        self.context.start()

    def stop(self):
        ...

    def shutdown(self, timeout: Optional[Union[int, float]] = None):
        ...


class ThreadNodeController(NodeController):

    context: threading.Thread
    running: bool

    def __init__(self, node_object: "Node", logger: logging.Logger):
        super().__init__(node_object, logger)

        # Create a thread to run the Node
        self.context = threading.Thread(target=self.node_object.run, args=(True,))

    def stop(self):
        self.node_object.running = False

    def shutdown(self, timeout: Optional[Union[int, float]] = None):

        if type(timeout) == type(None):
            timeout = config.get("worker.timeout.node-shutdown")

        self.stop()
        self.context.join(timeout=timeout)
        if self.context.is_alive():
            self.logger.error(
                f"Failed to JOIN thread controller for Node={self.node_object.state}"
            )


class MPNodeController(NodeController):

    context: mp.Process  # type: ignore
    running: mp.Value  # type: ignore

    def __init__(self, node_object: "Node", logger: logging.Logger):
        super().__init__(node_object, logger)

        # Create a process to run the Node
        self.running = mp.Value("i", True)  # type: ignore
        self.context = mp.Process(  # type: ignore
            target=self.node_object.run,
            args=(
                True,
                self.running,
            ),
        )

    def stop(self):
        self.running.value = False

    def shutdown(self, timeout: Optional[Union[int, float]] = None):

        if type(timeout) == type(None):
            timeout = config.get("worker.timeout.node-shutdown")

        self.stop()
        self.context.join(timeout=timeout)
        self.context.terminate()
