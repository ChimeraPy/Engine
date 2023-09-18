import typing
import logging
import abc
from typing import Any, Awaitable, Union, Optional

# Third-party Imports
import multiprocess as mp

from ...networking import DataChunk
from .context_session import ContextSession

if typing.TYPE_CHECKING:
    from ...node.node import Node

manager = mp.Manager()


class NodeController(abc.ABC):

    running: Union[bool, mp.Value]
    node_object: "Node"
    future: Optional[Awaitable]
    response: bool = False
    gather: DataChunk = DataChunk()
    registered_method_results: Any = None

    def __init__(self, node_object: "Node", logger: logging.Logger):

        # Save parameters
        self.node_object = node_object
        self.logger = logger
        self.future = None

    @abc.abstractmethod
    def run(self, context: ContextSession):
        ...

    @abc.abstractmethod
    def stop(self):
        ...

    async def shutdown(self):
        if self.future:
            self.stop()
            await self.future


class ThreadNodeController(NodeController):

    running: bool

    def __init__(self, node_object: "Node", logger: logging.Logger):
        super().__init__(node_object, logger)

    def run(self, context: ContextSession):
        self.future = context.add(self.node_object.run, (True,))

    def stop(self):
        self.node_object.running = False


class MPNodeController(NodeController):

    running: mp.Value  # type: ignore

    def __init__(self, node_object: "Node", logger: logging.Logger):
        super().__init__(node_object, logger)
        self.running = manager.Value("i", True)  # type: ignore

    def run(self, context: ContextSession):
        self.future = context.add(
            self.node_object.run,
            (
                True,
                self.running,
            ),
        )

    def stop(self):
        self.running.value = False
