import logging
from logging import Formatter, StreamHandler


class HandlerFactory:
    """Utility class to create logging handlers"""

    @staticmethod
    def get(name, level=logging.DEBUG) -> logging.Handler:
        if name == "console":
            hdlr = HandlerFactory.get_console_handler()
        else:
            raise ValueError(f"Unknown handler name: {name}")
        hdlr.setLevel(level)
        return hdlr

    @staticmethod
    def get_console_handler() -> logging.StreamHandler:
        console_handler = StreamHandler()
        console_handler.setFormatter(HandlerFactory.get_formatter())
        return console_handler

    @staticmethod
    def get_formatter() -> logging.Formatter:
        return Formatter(
            "%(asctime)s [%(levelname)s] %(name)s - %(object_id)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
