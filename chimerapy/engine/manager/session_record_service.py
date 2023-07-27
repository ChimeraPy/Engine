import datetime
import json
from typing import Optional, Dict

from ..states import ManagerState
from ..eventbus import EventBus, TypedObserver
from ..service import Service


class SessionRecordService(Service):
    def __init__(
        self,
        name: str,
        eventbus: EventBus,
        state: ManagerState,
    ):
        super().__init__(name=name)

        # Input parameters
        self.eventbus = eventbus
        self.state = state

        # State information
        self.start_time: Optional[datetime.datetime] = None
        self.stop_time: Optional[datetime.datetime] = None
        self.duration: float = 0

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "save_meta": TypedObserver(
                "save_meta", on_asend=self._save_meta, handle_event="drop"
            ),
            "start_recording": TypedObserver(
                "start_recording", on_asend=self.start_recording, handle_event="drop"
            ),
            "stop_recording": TypedObserver(
                "stop_recording", on_asend=self.stop_recording, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

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

        # Save the data
        self._save_meta()
