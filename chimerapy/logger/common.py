import logging

from logging import FileHandler, StreamHandler, Formatter


class HandlerFactory:
    """Utility class to create logging handlers"""

    @staticmethod
    def get(name, filename=None, level=logging.DEBUG):
        if name == "file" and not filename:
            raise ValueError("filename must be provided if handler is 'file'")
        if name == "console":
            hdlr = HandlerFactory.get_console_handler()
        elif name == "console-node_id":
            hdlr = HandlerFactory.get_node_id_context_console_handler()
        elif name == "console-ip":
            hdlr = HandlerFactory.get_hostname_context_console_handler()
        elif name == "file":
            hdlr = HandlerFactory.get_file_handler(filename)
        else:
            raise ValueError(f"Unknown handler name: {name}")
        hdlr.setLevel(level)
        return hdlr

    @staticmethod
    def get_console_handler():
        console_handler = StreamHandler()
        console_handler.setFormatter(HandlerFactory.get_formatter())
        return console_handler

    @staticmethod
    def get_node_id_context_console_handler():
        console_handler = StreamHandler()
        console_handler.setFormatter(HandlerFactory.get_node_id_formatter())
        return console_handler

    @staticmethod
    def get_hostname_context_console_handler():
        console_handler = StreamHandler()
        console_handler.setFormatter(
            HandlerFactory.get_formatter(include_hostname=True)
        )
        return console_handler

    @staticmethod
    def get_file_handler(log_file_dir):
        file_handler = FileHandler(f"{log_file_dir}/chimerapy.log")
        file_handler.setFormatter(HandlerFactory.get_formatter())
        return file_handler

    @staticmethod
    def get_formatter(include_hostname=False):
        if include_hostname:
            return Formatter(
                "%(asctime)s [%(levelname)s] %(name)s([%(hostname)s]): %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        else:
            return Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )

    @staticmethod
    def get_node_id_formatter():
        return Formatter(
            "%(asctime)s [%(levelname)s] %(name)s(NodeID-[%(node_id)s]): %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
