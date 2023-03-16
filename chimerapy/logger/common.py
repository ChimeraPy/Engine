import logging
from logging import Formatter, StreamHandler


class HandlerFactory:
    """Utility class to create logging handlers"""

    @staticmethod
    def get(name, level: int = logging.DEBUG) -> logging.Handler:
        if name == "console":
            hdlr = HandlerFactory.get_console_handler()
        elif name == "console-node_id":
            hdlr = HandlerFactory.get_node_id_context_console_handler()
        else:
            raise ValueError(f"Unknown handler name: {name}")
        hdlr.setLevel(level)
        return hdlr

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
