# Internal Imports
import threading
import queue
import logging
from typing import Dict, Optional

from chimerapy.engine import _logger
from ..states import NodeState
from ..eventbus import EventBus, TypedObserver
from ..records import (
    Record,
    VideoRecord,
    AudioRecord,
    TabularRecord,
    ImageRecord,
    JSONRecord,
    TextRecord,
)
from ..service import Service

logger = _logger.getLogger("chimerapy-engine")


class RecordService(Service):
    def __init__(
        self,
        name: str,
        state: NodeState,
        eventbus: EventBus,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(name)

        # Saving parameters
        self.state = state
        self.eventbus = eventbus

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

        # Put observers
        self.observers: Dict[str, TypedObserver] = {
            "setup": TypedObserver("setup", on_asend=self.setup, handle_event="drop"),
            "record": TypedObserver(
                "record", on_asend=self.record, handle_event="drop"
            ),
            "collect": TypedObserver(
                "collect", on_asend=self.collect, handle_event="drop"
            ),
            "teardown": TypedObserver(
                "teardown", on_asend=self.teardown, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    def setup(self):

        # self.logger.debug(f"{self}: executing main")
        self._record_thread = threading.Thread(target=self.run)
        self._record_thread.start()

    def record(self):
        # self.logger.debug(f"{self}: Starting recording")
        ...

    def teardown(self):

        # First, indicate the end
        self.is_running.clear()
        if self._record_thread:
            self._record_thread.join()

        # self.logger.debug(f"{self}: shutdown")

    ####################################################################
    ## Helper Methods & Attributes
    ####################################################################

    @property
    def enabled(self) -> bool:
        return self.state.fsm == "RECORDING"

    def submit(self, entry: Dict):
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

    def collect(self):
        # self.logger.debug(f"{self}: collecting recording")

        # Signal to stop and save
        self.is_running.clear()
        if self._record_thread:
            self._record_thread.join()

        # self.logger.debug(f"{self}: Finish saving records")
