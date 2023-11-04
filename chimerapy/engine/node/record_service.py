# Internal Imports
import logging
import queue
import threading
from typing import Dict, Optional

from aiodistbus import registry

from chimerapy.engine import _logger

from ..records import (
    AudioRecord,
    ImageRecord,
    JSONRecord,
    Record,
    TabularRecord,
    TextRecord,
    VideoRecord,
)
from ..service import Service
from ..states import NodeState

logger = _logger.getLogger("chimerapy-engine")


class RecordService(Service):
    def __init__(
        self,
        name: str,
        state: NodeState,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name)

        # Saving parameters
        self.state = state

        # State variables
        self.save_queue: queue.Queue = queue.Queue()

        # Saving thread state information
        self.is_running = threading.Event()
        self.is_running.set()

        # To keep record of entries
        self.records: Dict[str, Record] = {}
        self.record_map = {
            "video": VideoRecord,
            "audio": AudioRecord,
            "tabular": TabularRecord,
            "image": ImageRecord,
            "json": JSONRecord,
            "text": TextRecord,
        }

        # Making sure the attribute exists
        self._record_thread: Optional[threading.Thread] = None

        # Logging
        if logger:
            self.logger = logger
        else:
            self.logger = _logger.getLogger("chimerapy-engine")

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    @registry.on("setup", namespace=f"{__name__}.RecordService")
    def setup(self):

        # self.logger.debug(f"{self}: executing main")
        self._record_thread = threading.Thread(target=self.run)
        self._record_thread.start()

    @registry.on("record", namespace=f"{__name__}.RecordService")
    def record(self):
        # self.logger.debug(f"{self}: Starting recording")
        ...

    @registry.on("collect", namespace=f"{__name__}.RecordService")
    def collect(self):
        # self.logger.debug(f"{self}: collecting recording")

        # Signal to stop and save
        self.is_running.clear()
        if self._record_thread:
            self._record_thread.join()

        # self.logger.debug(f"{self}: Finish saving records")

    @registry.on("teardown", namespace=f"{__name__}.RecordService")
    def teardown(self):

        # First, indicate the end
        self.is_running.clear()
        if self._record_thread:
            self._record_thread.join()

        # self.logger.debug(f"{self}: shutdown")

    ####################################################################
    ## Helper Methods & Attributes
    ####################################################################

    @registry.on("record_entry", Dict, namespace=f"{__name__}.RecordService")
    def submit(self, entry: Dict):

        # self.logger.debug(f"{self}: Received data: {entry}")

        if self.state.fsm != "RECORDING":
            return None

        self.save_queue.put(entry)

    def run(self):

        # self.logger.debug(f"{self}: Running poll threading, {self.state.logdir}")

        # Continue checking for messages from client until not running
        while self.is_running.is_set() or self.save_queue.qsize() != 0:

            # Received data to save
            try:
                # self.logger.debug(F"{self}: Checking save_queue")
                data_entry = self.save_queue.get(timeout=1)
            except queue.Empty:
                continue

            # Case 1: new entry
            if data_entry["name"] not in self.records:
                entry_cls = self.record_map[data_entry["dtype"]]
                entry = entry_cls(dir=self.state.logdir, name=data_entry["name"])

                # FixMe: Potential overwrite of existing entry?
                self.records[data_entry["name"]] = entry

            # Case 2
            # self.logger.debug(
            #     f"{self}: Writing data entry for {data_entry['name']}"
            # )
            self.records[data_entry["name"]].write(data_entry)

        # Ensure that all entries close
        for entry in self.records.values():
            entry.close()

        # self.logger.debug(f"{self}: Closed all entries")
