# Built-in Imports
import threading
import socket
import logging
import pickle

# Internal Imports
from . import _logger

class LogReceiver(threading.Thread):

    def __init__(self, port:int = 0, logger_name:str = ""):
        super().__init__()

        # Save input parameters
        self.port: int = port
        self.logger_name: str = logger_name

        # Create server and logger to relay messages
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s.bind(("127.0.0.1", self.port))
        self.s.setblocking(0)
        self.s.settimeout(2)
        self.logger = _logger.getLogger(self.logger_name)

        # If port = 0, that means that a random port was selected,
        # therefore, let's get it
        self.port = self.s.getsockname()[1]

        # Saving thread state information
        self.is_running = threading.Event()
        self.is_running.set()

    def set_logging_level(self, logging_level: int):
        self.logger.set_logging_level(logging_level)

    def run(self):

        # Continue listening until signaled to stop
        while self.is_running.is_set():

            # Listen for information
            try:
                data = self.s.recv(4096)
            except socket.timeout:
                continue

            # Dont forget to skip over the 32-bit length prepended
            logrec = pickle.loads(data[4:])
            rec = logging.LogRecord(
                name=logrec["name"],
                level=logrec["levelno"],
                pathname=logrec["pathname"],
                lineno=logrec["lineno"],
                msg=logrec["msg"],
                args=logrec["args"],
                exc_info=logrec["exc_info"],
                func=logrec["funcName"],
            )
            
            # Only handle if the rec matches the logger's level
            if rec.levelno >= self.logger.level:
                self.logger.handle(rec)

        # Safely shutdown socket
        self.s.close()

    def shutdown(self):

        # Indicate end
        self.is_running.clear()