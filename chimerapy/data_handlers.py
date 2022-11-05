from typing import Dict, List
import pathlib
import logging
import threading
import queue

logger = logging.getLogger("chimerapy")

from . import enums


class SaveHandler(threading.Thread):
    def __init__(self, logdir: pathlib.Path, save_queue: queue.Queue):

        # Save input parameters
        self.logdir = logdir
        self.save_queue = save_queue

        # Saving thread state information
        self.is_running = threading.Event()
        self.is_running.set()

    def run(self):

        # Continue checking for messages from client until not running
        while self.is_running.is_set():
            ...

    def shutdown(self):

        # First, indicate the end
        self.is_running.clear()


class OutputsHandler(threading.Thread):
    def __init__(
        self, name: str, out_queue: queue.Queue, out_bound: List, p2p_clients: Dict
    ):
        super().__init__()

        # Save input parameters
        self.name = name
        self.out_queue = out_queue
        self.out_bound = out_bound
        self.p2p_clients = p2p_clients

        # Saving thread state information
        self.is_running = threading.Event()
        self.is_running.set()

    def run(self):

        # If there is no out_bound nodes, just stop
        if len(self.out_bound) == 0:
            return None

        # Continue checking for messages from client until not running
        while self.is_running.is_set():

            # Check if there is some outputs to be send!
            try:
                outputs = self.out_queue.get(timeout=1)
            except queue.Empty:
                continue

            # Send the message to all p2p clients
            for out_bound_name, client in self.p2p_clients.items():
                client.send(
                    {
                        "signal": enums.NODE_DATA_TRANSFER,
                        "data": {
                            "sent_from": self.name,
                            "send_to": out_bound_name,
                            "outputs": outputs,
                        },
                    },
                )

    def shutdown(self):

        # First, indicate the end
        self.is_running.clear()
