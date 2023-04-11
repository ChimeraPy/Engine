from typing import Dict, List
import os
import logging
import threading
import queue
import uuid

from .. import _logger

logger = _logger.getLogger("chimerapy")

# Internal Imports
from ..records import VideoRecord, AudioRecord, TabularRecord, ImageRecord
from .node_service import NodeService


class RecordService(NodeService):

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    def setup(self):

        self.node.logger.debug(f"{self}: setup executed")

        # Create IO queue
        self.save_queue = queue.Queue()

        # Saving thread state information
        self.is_running = threading.Event()
        self.is_running.set()

        # To keep record of entries
        self.records = {}
        self.record_map = {
            "video": VideoRecord,
            "audio": AudioRecord,
            "tabular": TabularRecord,
            "image": ImageRecord,
        }

        # Creating thread for saving incoming data
        os.makedirs(
            self.node.logdir, exist_ok=True
        )  # Triple-checking that it's there (Issue #155)
        self._thread = threading.Thread(target=self.run)
        self._thread.start()

    def teardown(self):

        # First, indicate the end
        self.is_running.clear()
        self._thread.join()

        self.node.logger.debug(f"{self}: shutdown")

    ####################################################################
    ## Helper Methods & Attributes
    ####################################################################

    @property
    def enabled(self) -> bool:
        return self.node.state.fsm == "RUNNING"

    def submit(self, entry: Dict):
        self.save_queue.put(entry)

    def run(self):

        self.node.logger.debug(f"{self}: Running poll threading")

        # Continue checking for messages from client until not running
        while self.is_running.is_set() or self.save_queue.qsize() != 0:

            # Received data to save
            try:
                data_entry = self.save_queue.get(timeout=1)
            except queue.Empty:
                continue

            # Case 1: new entry
            if data_entry["name"] not in self.records:
                entry_cls = self.record_map[data_entry["dtype"]]
                entry = entry_cls(dir=self.node.logdir, name=data_entry["name"])
                self.records[data_entry["name"]] = entry

            # Case 2
            # self.node.logger.debug(
            #     f"{self}: Writing data entry for {data_entry['name']}"
            # )
            self.records[data_entry["name"]].write(data_entry)

        # Ensure that all entries close
        for entry in self.records.values():
            entry.close()

    def save(self):

        # Signal to stop and save
        self.is_running.clear()
        self._thread.join()

        self.node.logger.debug(f"{self}: Finish saving records")
