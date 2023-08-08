# Internal Imports
import os
import logging
import uuid
from typing import Dict, Optional

from chimerapy.engine import _logger
from ...states import NodeState
from ...eventbus import EventBus, TypedObserver
from ...service import Service
from .recording import Recording
from .entry import Entry

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

        # Containers
        self.current_recording_key: Optional[str] = None
        self.recordings: Dict[str, Recording] = {}

        # Creating thread for saving incoming data
        os.makedirs(
            self.state.logdir, exist_ok=True
        )  # Triple-checking that it's there (Issue #155)

        # Logging
        if logger:
            self.logger = logger
        else:
            self.logger = _logger.getLogger("chimerapy-engine")

        # Put observers
        self.observers: Dict[str, TypedObserver] = {
            "record": TypedObserver(
                "record", on_asend=self.record, handle_event="drop"
            ),
            "stop": TypedObserver("stop", on_asend=self.stop, handle_event="drop"),
            "collect": TypedObserver(
                "collect", on_asend=self.collect, handle_event="drop"
            ),
            "teardown": TypedObserver(
                "teardown", on_asend=self.collect, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    ####################################################################
    ## Lifecycle Hooks
    ####################################################################

    def record(self, recording_uuid: Optional[str] = None) -> Optional[Recording]:

        # First check that their isn't another recording
        if self.current_recording_key:
            self.logger.error(f"{self}: New recording while still recording!")
            return None

        # Handle inputs
        if not recording_uuid:
            recording_uuid = str(uuid.uuid4())

        # Start a new recording
        recording = Recording(
            dir=self.state.logdir / recording_uuid, logger=self.logger
        )

        # Store
        self.recordings[recording_uuid] = recording
        self.current_recording_key = recording_uuid

        return recording

    def stop(self, blocking: bool = False):

        # Only stop current recording
        if not self.current_recording_key:
            return None

        # Stop the current recording
        current_recording = self.recordings[self.current_recording_key]
        current_recording.stop(blocking=False)
        self.current_recording_key = None

    def collect(self):

        # Now all recordings must stop!
        for recording in self.recordings.values():
            recording.stop(blocking=True)

        # Update the information
        self.current_recording_key = None

    ####################################################################
    ## Helper Methods & Attributes
    ####################################################################

    @property
    def enabled(self) -> bool:
        return self.state.fsm == "RECORDING"

    def submit(self, entry: Entry):

        # Only if recording working
        if not self.current_recording_key:
            return None

        # Obtain the recording
        recording = self.recordings[self.current_recording_key]
        recording.submit(entry)
