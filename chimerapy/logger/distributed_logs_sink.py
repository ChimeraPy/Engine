import logging
from datetime import datetime
from logging import Handler, Logger, Filter
from pathlib import Path
from typing import Union, Callable, Optional, Sequence

from .common import HandlerFactory
from .zmq_handlers import ZMQPullListener


class MultiplexedFileHandler(Handler):
    def __init__(self, name):
        super().__init__()
        self.handlers = {}
        self.set_name(f"{name}")

    def initialize_entity(self, prefix: str, identifier: str, parent_dir: Path):
        """Register this handler to"""
        handler = HandlerFactory.get(
            "file",
            filename=str(parent_dir / f"{prefix}_{identifier}_{self.timestamp()}.log"),
        )
        self.handlers[identifier] = handler

    def emit(self, record):
        if hasattr(record, "identifier"):
            handler = self.handlers.get(record.identifier)
            if handler is not None:
                handler.emit(record)

    @staticmethod
    def timestamp() -> str:
        return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


HANDLERS_REGISTRY = {"file": MultiplexedFileHandler}


class DistributedLogsMultiplexedFileSink:
    """Collects logs from all the and saves them to a file handler."""

    def __init__(self, port: Optional[int]) -> None:
        self.handler = MultiplexedFileHandler("MultiplexedFileHandler")
        self.listener = ZMQPullListener(port=port, handlers=[self.handler])

    @property
    def port(self):
        return self.listener.port

    def start(self):
        self.listener.start()

    def initialize_entity(self, name, identifier, parent_dir):
        self.handler.initialize_entity(name, identifier, parent_dir)

    def shutdown(self):
        self.listener.stop()
        self.listener.join()
