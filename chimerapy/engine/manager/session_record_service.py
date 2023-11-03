import datetime
import json
from typing import Dict, Optional

from aiodistbus import registry

from ..service import Service
from ..states import ManagerState


class SessionRecordService(Service):
    def __init__(
        self,
        name: str,
        state: ManagerState,
    ):
        super().__init__(name=name)

        # Input parameters
        self.state = state

        # State information
        self.start_time: Optional[datetime.datetime] = None
        self.stop_time: Optional[datetime.datetime] = None
        self.duration: float = 0

    @registry.on("save_meta", namespace=f"{__name__}.SessionRecordService")
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
            # "nodes": list(self.services.worker_handler.graph.G.nodes()),
            # "worker_graph_map": self.services.worker_handler.worker_graph_map,
            # "nodes_server_table": self.services.worker_handler.nodes_server_table,
            "start_time": start_time,
            "stop_time": stop_time,
        }

        with open(self.state.logdir / "meta.json", "w") as f:
            json.dump(meta, f, indent=2)

    @registry.on("start_recording", namespace=f"{__name__}.SessionRecordService")
    def start_recording(self):

        # Mark the start time
        self.start_time = datetime.datetime.now()

    @registry.on("stop_recording", namespace=f"{__name__}.SessionRecordService")
    def stop_recording(self):

        # Mark the stop time
        self.stop_time = datetime.datetime.now()

        if isinstance(self.start_time, datetime.datetime):
            self.duration = (self.stop_time - self.start_time).total_seconds()
        else:
            self.duration = 0

        # Save the data
        self._save_meta()
