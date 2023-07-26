import atexit
import logging
import os
import threading
from logging import LogRecord, makeLogRecord
from logging.handlers import QueueHandler
from typing import Collection, Optional, Callable, Dict, Any

import zmq

from .common import HandlerFactory
from .utils import bind_pull_socket, connect_push_socket


class ZMQPullListener(threading.Thread):
    """A thread that listens for log messages.

    This subclass of threading.Thread is used to listen for log messages on a ZMQ \
    socket and pass them to the handlers.

    Args:
        port (int, optional): The port to listen on. If None, a random port will be \
            chosen.
        handlers (list of logging.Handlers, optional): The handlers to pass the log \
            messages to. If None, a console handler will be used.
        respect_handler_level (bool, optional): If True, only pass log messages to \
            handlers that have a level. Defaults to True.

    See Also:
        chimerapy.logger.utils.bind_pull_socket
            The function used to bind the socket.
    """

    _sentinel = {"msg": "STOP"}

    def __init__(
        self,
        port: Optional[int] = None,
        handlers=None,
        respect_handler_level: bool = True,
    ):
        super().__init__()
        socket, port = bind_pull_socket(port)
        self.push_queue = connect_push_socket("127.0.0.1", port)
        self.handlers = handlers or [
            HandlerFactory.get("console")
        ]  # For now, only console handler
        self.running = threading.Event()
        self.port = port
        self.queue = socket
        self.respect_handler_level = respect_handler_level
        self.running.set()
        self.daemon = True

    def run(self) -> None:
        """Run the LogsListener thread."""
        while self.running.is_set():
            try:
                logobj = self.queue.recv_json(zmq.NOBLOCK)

                if "msg" in logobj and logobj["msg"] == "STOP":
                    break

                record = makeLogRecord(logobj)

                for handler in self.handlers:
                    if self.respect_handler_level and record.levelno < handler.level:
                        continue
                    handler.handle(record)

            except zmq.Again:
                continue

    def stop(self) -> None:
        """Stop the LogsListener thread."""
        self._enqueue_sentinel()
        self.running.clear()

    def _enqueue_sentinel(self) -> None:
        """Enqueue the sentinel message to stop the thread's run loop."""
        self.push_queue.send_json(self._sentinel)

    def start(self, register_exit_handlers=False) -> None:
        """Start the LogsListener thread by optionally registering exit handlers."""
        self.running.set()
        super().start()
        if register_exit_handlers:
            # Note the order of these two calls.
            atexit.register(self.join)
            atexit.register(self.stop)

    def add_handler(self, handler):
        with threading.Lock():
            self.handlers.append(handler)

    addHandler = add_handler  # Duck typing with logging.Logger


class NodeIDZMQPullListener(ZMQPullListener):
    """A thread that listens for log messages and adds the node_id formatted console \
    handler."""

    def __init__(
        self,
        port: Optional[int] = None,
        handlers: Collection[logging.Handler] = None,
        respect_handler_level: bool = True,
    ):
        if handlers is None:
            handlers = [HandlerFactory.get("console-node_id")]

        super().__init__(port, handlers, respect_handler_level=respect_handler_level)


class ZMQPushHandler(QueueHandler):
    """A handler that sends log messages to a ZMQ PUSH socket."""

    def __init__(self, host: str, port: int):
        socket = connect_push_socket(host, port)
        super().__init__(socket)

    def emit(self, record: LogRecord) -> None:
        self.queue.send_json(record.__dict__)  # type: ignore[union-attr]


class NodeIdZMQPushHandler(ZMQPushHandler):
    """A handler that sends log messages to a ZMQ PUSH socket and adds the node_id to \
    the record.

    Note:
        The node_id is added to the record as an attribute. The default node_id is \
        queried by the process id.
    """

    def __init__(
        self, host: str, port: int, node_id_callback: Optional[Callable] = os.getpid
    ):
        super().__init__(host, port)
        self.node_id_callback = node_id_callback
        self.node_ids: Dict[Any, str] = {}

    def emit(self, record: LogRecord) -> None:
        return_callback = None
        if self.node_id_callback:
            return_callback = self.node_id_callback()
        record.node_id = self.node_ids.get(return_callback, None)  # type: ignore
        super().emit(record)

    def register_node_id(self, node_id: str) -> None:
        return_callback = None
        if self.node_id_callback:
            return_callback = self.node_id_callback()
        self.node_ids[return_callback] = node_id
