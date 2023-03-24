from typing import Optional

from .common import HandlerFactory
from .zmq_handlers import ZMQPullListener


class DistributedLogsMultiplexedFileSink:
    """Collects logs from all the and saves them to a file handler."""

    def __init__(self, port: Optional[int], **handler_kwargs) -> None:
        self.handler = HandlerFactory.get("multiplexed-rotating-file", **handler_kwargs)
        self.listener = ZMQPullListener(port=port, handlers=[self.handler])

    @property
    def port(self):
        return self.listener.port

    def start(self, register_exit_handlers: bool = False) -> None:
        """Start the listener and register the exit handlers if requested."""
        self.listener.start(register_exit_handlers)

    def initialize_entity(self, name, identifier, parent_dir) -> None:
        """Register a logging entity with the given identifier, thereby creating a new file handler for it."""
        self.handler.initialize_entity(name, identifier, parent_dir)

    def deregister_entity(self, identifier: str) -> None:
        """Deregister a logging entity with the given identifier, thereby closing the file handler for it."""
        self.handler.deregister_entity(identifier)

    def shutdown(self):
        """Shutdown the listener."""
        self.listener.stop()
        self.listener.join()
