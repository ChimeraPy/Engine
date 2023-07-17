import datetime
import random
import pathlib
import os
import json
from typing import Optional, Union

from .manager_service import ManagerService


class SessionRecordService(ManagerService):
    def __init__(self, name: str, logdir: Union[str, pathlib.Path]):
        super().__init__(name=name)

        # State information
        self.start_time: Optional[datetime.datetime] = None
        self.stop_time: Optional[datetime.datetime] = None
        self.duration: float = 0

        # Create log directory to store data
        self.timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        self.rand_num = random.randint(1000, 9999)
        self.logdir = (
            pathlib.Path(logdir).resolve()
            / f"chimerapy-{self.timestamp}-{self.rand_num}"
        )

        # Create a logging directory
        os.makedirs(self.logdir, exist_ok=True)

    def _save_meta(self):
        # Get the times, handle Optional
        if self.start_time:
            start_time = self.start_time.strftime("%Y_%m_%d_%H_%M_%S.%f%z")
        else:
            start_time = None

        if self.stop_time:
            stop_time = self.stop_time.strftime("%Y_%m_%d_%H_%M_%S.%f%z")
        else:
            stop_time = None

        # Generate meta record
        meta = {
            "workers": list(self.state.workers.keys()),
            "nodes": list(self.services.worker_handler.graph.G.nodes()),
            "worker_graph_map": self.services.worker_handler.worker_graph_map,
            "nodes_server_table": self.services.worker_handler.nodes_server_table,
            "start_time": start_time,
            "stop_time": stop_time,
        }

        with open(self.logdir / "meta.json", "w") as f:
            json.dump(meta, f, indent=2)

    def start_recording(self):

        # Mark the start time
        self.start_time = datetime.datetime.now()

    def stop_recording(self):

        # Mark the stop time
        self.stop_time = datetime.datetime.now()

        if isinstance(self.start_time, datetime.datetime):
            self.duration = (self.stop_time - self.start_time).total_seconds()
        else:
            self.duration = 0
