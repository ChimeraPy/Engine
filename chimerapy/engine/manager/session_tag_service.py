import datetime
import json
from typing import Optional, Dict

from ..states import ManagerState
from ..eventbus import EventBus, TypedObserver
from ..service import Service
from .events import TagEvent


class SessionTagService(Service):
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
        self.tags: Dict[str, Dict] = {}

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "start_recording": TypedObserver(
                "start_recording", on_asend=self._record_start_time, handle_event="drop"
            ),
            "create_tag": TypedObserver(
                "create_tag",
                event_data_cls=TagEvent,
                on_asend=self._create_tag,
                handle_event="unpack",
            ),
            "update_tag": TypedObserver(
                "update_tag",
                event_data_cls=TagEvent,
                on_asend=self._update_tag,
                handle_event="unpack",
            ),
            "stop_recording": TypedObserver(
                "stop_recording", on_asend=self._consolidate_tags, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    def can_create_tag(self):
        all_node_states = list(
            node_state.fsm
            for worker in self.state.workers.values()
            for node_state in worker.nodes.values()
        )

        if len(all_node_states) == 0:
            return False, "No nodes to tag"

        can_create = all(node_state == "RECORDING" for node_state in all_node_states)
        reason = (
            "All nodes must be in RECORDING state to add a tag"
            if not can_create
            else None
        )
        return can_create, reason

    def _record_start_time(self):
        self.start_time = datetime.datetime.now()

    def get_elapsed_time(self):
        delta = datetime.datetime.now() - self.start_time
        return delta.total_seconds()

    def _create_tag(self, uuid, name, description=None):
        e_time = self.get_elapsed_time()
        self.tags[uuid] = {
            "uuid": uuid,
            "name": name,
            "description": description,
            "timestamp": e_time,
            "timestamp_str": f"{e_time // 3600} Hours, "
            f"{(e_time // 60) % 60} Minutes, "
            f"{e_time % 60} Seconds",
        }

    def _update_tag(self, uuid, name, description):
        if uuid in self.tags:
            self.tags[uuid]["name"] = name
            self.tags[uuid]["description"] = description

    def get_tag_name(self, uuid):
        if uuid in self.tags:
            return self.tags[uuid]["name"]
        else:
            return None

    def _consolidate_tags(self):
        with (self.state.logdir / "session_tags.json").open("w") as f:
            json.dump(self.tags, f, indent=2)
