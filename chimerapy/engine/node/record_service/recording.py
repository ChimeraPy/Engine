import pathlib
import os
import queue
import datetime
import threading
import logging
from typing import Dict, Union, Optional

from chimerapy.engine import _logger
from .records import Record, VideoRecord, AudioRecord, TabularRecord, ImageRecord
from .entry import Entry, VideoEntry, AudioEntry, TabularEntry, ImageEntry

logger = _logger.getLogger("chimerapy-engine")


class Recording:
    def __init__(self, dir: pathlib.Path, logger: Optional[logging.Logger] = None):

        # Save input variables
        self.dir = dir

        # Make sure the recording directory exists
        os.makedirs(self.dir, exist_ok=True)

        # Logging
        if logger:
            self.logger = logger
        else:
            self.logger = _logger.getLogger("chimerapy-engine")

        # State variables
        self.save_queue: queue.Queue = queue.Queue()
        self.running: bool = True
        self.start_time: datetime.datetime = datetime.datetime.now()
        self.stop_time: Optional[datetime.datetime] = None

        # To keep record of entries
        self.records: Dict[str, Union[Record]] = {}
        self.record_entry_map = {
            VideoEntry: VideoRecord,
            AudioEntry: AudioRecord,
            TabularEntry: TabularRecord,
            ImageEntry: ImageRecord,
        }

        # Making sure the attribute exists
        self._record_thread = threading.Thread(target=self.run)
        self._record_thread.start()

    # def _save_meta(self):
    #     # Get the times, handle Optional
    #     if self.start_time:
    #         start_time = self.start_time.strftime("%Y_%m_%d_%H_%M_%S.%f%z")
    #     else:
    #         start_time = None

    #     if self.stop_time:
    #         stop_time = self.stop_time.strftime("%Y_%m_%d_%H_%M_%S.%f%z")
    #     else:
    #         stop_time = None

    #     # Generate meta record
    #     meta = {
    #         "workers": list(self.state.workers.keys()),
    #         # "nodes": list(self.services.worker_handler.graph.G.nodes()),
    #         # "worker_graph_map": self.services.worker_handler.worker_graph_map,
    #         # "nodes_server_table": self.services.worker_handler.nodes_server_table,
    #         "start_time": start_time,
    #         "stop_time": stop_time,
    #     }

    #     with open(self.state.logdir / "meta.json", "w") as f:
    #         json.dump(meta, f, indent=2)

    def submit(self, entry: Entry):
        self.save_queue.put(entry)

    def run(self):

        # self.logger.debug(f"{self}: Running poll threading")

        # Continue checking for messages from client until not running
        while self.running or self.save_queue.qsize() != 0:

            # Received data to save
            try:
                # self.logger.debug(F"{self}: Checking save_queue")
                data_entry = self.save_queue.get(timeout=1)
            except queue.Empty:
                continue

            # Case 1: new entry
            if data_entry.name not in self.records:
                record_cls = self.record_entry_map[data_entry.__class__]
                record = record_cls(
                    dir=self.dir, name=data_entry.name, start_time=self.start_time
                )
                self.records[data_entry.name] = record

            # Case 2
            # self.logger.debug(
            #     f"{self}: Writing data entry for {data_entry['name']}"
            # )
            self.records[data_entry.name].write(data_entry)

        # Ensure that all entries close
        for entry in self.records.values():
            entry.close()

        # self.logger.debug(f"{self}: Closed all entries")

    def stop(self, blocking: bool = False):

        # Stop running
        self.running = False

        if blocking:
            self._record_thread.join()
