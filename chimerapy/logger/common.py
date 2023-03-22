import logging
from logging import Formatter, StreamHandler, Filter
from logging.handlers import RotatingFileHandler


class HandlerFactory:
    """Utility class to create logging handlers"""

    @staticmethod
    def get(
        name, *, filename: str = None, level: int = logging.DEBUG
    ) -> logging.Handler:
        if name == "file" and filename is None:
            raise ValueError("filename must be provided for file handler")
        if name == "console":
            hdlr = HandlerFactory.get_console_handler()
        elif name == "file":
            hdlr = HandlerFactory.get_file_handler(filename)
        elif name == "console-node_id":
            hdlr = HandlerFactory.get_node_id_context_console_handler()
        else:
            raise ValueError(f"Unknown handler name: {name}")
        hdlr.setLevel(level)
        return hdlr

    @staticmethod
    def get_file_handler(filename: str) -> logging.FileHandler:
        file_handler = RotatingFileHandler(filename)
        file_handler.setFormatter(HandlerFactory.get_vanilla_formatter())
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
        record.identifier = self.identifier
        return True
