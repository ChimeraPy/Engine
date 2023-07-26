import logging
from datetime import datetime
from logging import Filter, Formatter, Handler, StreamHandler
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Union, Dict

MAX_BYTES_PER_FILE = 100 * 1024 * 1024  # 100MB


class HandlerFactory:
    """Utility class to create logging handlers."""

    @staticmethod
    def get(
        name,
        *,
        filename: str = None,
        max_bytes: int = MAX_BYTES_PER_FILE,
        level: int = logging.DEBUG,
    ) -> Union[
        logging.Handler,
        "MultiplexedEntityHandler",
        "MultiplexedRotatingFileHandler",
        "StreamHandler",
    ]:
        if name in {"rotating-file"} and filename is None:
            raise ValueError("filename must be provided for file handler(s)")
        if name == "console":
            hdlr = HandlerFactory.get_console_handler()
        elif name == "rotating-file" and filename:
            hdlr = HandlerFactory.get_rotating_file_handler(filename, max_bytes)
        elif name == "multiplexed-rotating-file":
            hdlr = HandlerFactory.get_multiplexed_file_handler(
                name, max_bytes
            )  # type: ignore[assignment]
        elif name == "console-node_id":
            hdlr = HandlerFactory.get_node_id_context_console_handler()
        else:
            raise ValueError(f"Unknown handler name: {name}")
        hdlr.setLevel(level)
        return hdlr

    @staticmethod
    def get_rotating_file_handler(filename: str, max_bytes: int) -> RotatingFileHandler:
        file_handler = RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=20)
        file_handler.setFormatter(HandlerFactory.get_vanilla_formatter())
        return file_handler

    @staticmethod
    def get_multiplexed_file_handler(
        filename: str, max_bytes: int
    ) -> "MultiplexedRotatingFileHandler":
        file_handler = MultiplexedRotatingFileHandler(filename, max_bytes=max_bytes)
        return file_handler

    @staticmethod
    def get_console_handler() -> StreamHandler:
        console_handler = StreamHandler()
        console_handler.setFormatter(HandlerFactory.get_vanilla_formatter())
        return console_handler

    @staticmethod
    def get_node_id_context_console_handler() -> StreamHandler:
        console_handler = StreamHandler()
        console_handler.setFormatter(HandlerFactory.get_node_id_formatter())
        return console_handler

    @staticmethod
    def get_vanilla_formatter() -> Formatter:
        return Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    @staticmethod
    def get_node_id_formatter() -> Formatter:
        return Formatter(
            "%(asctime)s [%(levelname)s] %(name)s(NodeID-[%(node_id)s]): %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


class IdentifierFilter(Filter):
    """Filter to add an identifier to the log record."""

    def __init__(self, identifier: str):
        super().__init__()
        self.identifier = identifier

    def filter(self, record: logging.LogRecord) -> bool:
        record.identifier = self.identifier  # type: ignore
        return True


class MultiplexedEntityHandler(Handler):
    """An abstract class for handlers that multiplex the log messages to different \
    handlers based on the identifier on LogRecord.
    """

    def __init__(self, name):
        super().__init__()
        self.set_name(f"{name}")

    def initialize_entity(self, prefix: str, identifier: str, parent_dir: Path) -> None:
        """Register an entity with the given identifier."""
        ...

    def deregister_entity(self, identifier: str):
        """Deregister this handler from the entity."""
        ...

    def emit(self, record: logging.LogRecord):
        """Emit the record to the appropriate handler."""
        ...

    @staticmethod
    def timestamp() -> str:
        """Return the current timestamp in the format YYYY-MM-DD_HH-MM-SS."""
        return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


class MultiplexedRotatingFileHandler(MultiplexedEntityHandler):
    """A logging handler that multiplexes the log messages to different files based on \
    the identifier on LogRecord.
    """

    def __init__(self, name, max_bytes: int = MAX_BYTES_PER_FILE):
        super().__init__(name)
        self.handlers: Dict[str, logging.Handler] = {}
        self.max_bytes_per_file = max_bytes

    def initialize_entity(self, prefix: str, identifier: str, parent_dir: Path) -> None:
        """Register an entity with the given identifier, thereby creating a new file \
        handler for it.
        """
        handler = HandlerFactory.get(
            "rotating-file",
            filename=str(parent_dir / f"{prefix}_{identifier}_{self.timestamp()}.log"),
            max_bytes=self.max_bytes_per_file,
        )
        self.handlers[identifier] = handler

    def deregister_entity(self, identifier: str) -> None:
        """Deregister this handler from the entity, thereby closing the file handler \
        for it.
        """
        handler = self.handlers.pop(identifier, None)
        if handler is not None:
            handler.close()

    def emit(self, record: logging.LogRecord) -> None:
        """Emit the record to the appropriate file handler."""
        if hasattr(record, "identifier"):
            handler = self.handlers.get(record.identifier)  # type: ignore
            if handler is not None:
                handler.emit(record)
