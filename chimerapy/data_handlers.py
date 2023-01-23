from typing import Dict, List
import pathlib
import logging
import threading
import queue
import uuid

from . import _logger

logger = _logger.getLogger("chimerapy")

# Internal Imports
from .records import VideoRecord, AudioRecord, TabularRecord, ImageRecord


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
