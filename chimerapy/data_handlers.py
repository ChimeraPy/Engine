from typing import Dict, List
import pathlib
import logging
import threading
import queue
import uuid

from . import _logger

logger = _logger.getLogger("chimerapy")

# Internal Imports
from . import enums
from .records import VideoRecord, AudioRecord, TabularRecord, ImageRecord
from .utils import create_payload


class SaveHandler(threading.Thread):
    def __init__(self, logdir: pathlib.Path, save_queue: queue.Queue):
        super().__init__()

        # Save input parameters
        self.logdir = logdir
        self.save_queue = save_queue

        # To keep record of entries
        self.records = {}
        self.record_map = {
            "video": VideoRecord,
            "audio": AudioRecord,
            "tabular": TabularRecord,
            "image": ImageRecord,
        }

        # Saving thread state information
        self.is_running = threading.Event()
        self.is_running.set()

    def run(self):

        # Continue checking for messages from client until not running
        while self.is_running.is_set() or self.save_queue.qsize() != 0:

            # Received data to save
            try:
                data_chunk = self.save_queue.get(timeout=1)
            except queue.Empty:
                continue

            # Case 1: new entry
            if data_chunk["name"] not in self.records:
                entry_cls = self.record_map[data_chunk["dtype"]]
                entry = entry_cls(dir=self.logdir, name=data_chunk["name"])
                self.records[data_chunk["name"]] = entry

            # Case 2
            self.records[data_chunk["name"]].write(data_chunk)

        # Ensure that all entries close
        for entry in self.records.values():
            entry.close()

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

    def __str__(self):
        return f"<OutputsHandler for Node={self.name}>"

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

            # Create payload once and send to clients
            # Create msg_uuid
            msg_uuid = str(uuid.uuid4())

            msg = {
                "signal": enums.NODE_DATA_TRANSFER,
                "data": {
                    "sent_from": self.name,
                    "outputs": outputs,
                },
            }

            # First create payload and then use send it via all client sockets
            msg_bytes, msg_length = create_payload(
                type=enums.NODE_MESSAGE,
                signal=msg["signal"],
                data=msg["data"],
                provided_uuid=msg_uuid,
                ack=False,
            )

            # Send the message to all p2p clients
            for out_bound_name, client in self.p2p_clients.items():

                # We don't care about the futures
                # logger.debug(f"{self}: Sending to {out_bound_name}")
                _ = client.send_bytes(msg=msg_length + msg_bytes, msg_uuid=msg_uuid)

    def shutdown(self):

        # First, indicate the end
        self.is_running.clear()
