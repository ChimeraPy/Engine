# Internal Imports
import logging
import queue
import threading
from typing import Dict, Optional

from chimerapy.engine import _logger

from ..eventbus import EventBus, TypedObserver
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
from .events import Artifact
from ..eventbus import Event

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

    async def async_init(self):

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
            await self.eventbus.asubscribe(ob)

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    async def setup(self):

        # self.logger.debug(f"{self}: executing main")
        self._record_thread = threading.Thread(target=self.run)
        self._record_thread.start()

    async def record(self):
        # self.logger.debug(f"{self}: Starting recording")
        ...

    async def teardown(self):

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

    async def collect(self):
        # self.logger.debug(f"{self}: collecting recording")

        # Signal to stop and save
        self.is_running.clear()
        artifacts = {}
        if self._record_thread:
            self._record_thread.join()

        for name, entry in self.records.items():
            artifacts[name] = entry.get_meta()

        all_artifacts = []
        self.logger.info("Sending artifacts event")
        for name, artifact in artifacts.items():
            event_data = Artifact(
                name=artifact["name"],
                mime_type=artifact["mime_type"],
                path=artifact["path"],
                glob=artifact["glob"],
                size=artifact["path"].stat().st_size,
            )
            all_artifacts.append(event_data)

        await self.eventbus.asend(Event("artifacts", data=all_artifacts))
        self.logger.debug("Sent artifacts event")

        # self.logger.debug(f"{self}: Finish saving records")
