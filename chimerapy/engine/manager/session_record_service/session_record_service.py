import uuid
from dataclasses import dataclass, field
from typing import Optional, Dict

import aioshutil

from chimerapy.engine import _logger
from ...states import ManagerState
from ...eventbus import EventBus, TypedObserver, Event
from ...service import Service
from ..events import RecordEvent, SessionFilesEvent
from .recording import Recording

logger = _logger.getLogger("chimerapy-engine")


@dataclass
class RecordingTable:
    current_recording: Optional[Recording] = None
    table: Dict[str, Recording] = field(default_factory=dict)


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
        self.recordings = RecordingTable()

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "session_files": TypedObserver(
                "session_files",
                SessionFilesEvent,
                on_asend=self.session_files_formatting,
                handle_event="unpack",
            )
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    async def record(self, rec_uuid: Optional[str] = None) -> bool:

        # Check that there isn't a current recording
        if self.recordings.current_recording:
            logger.error(f"{self}: Cannot start a new recording, while recording!")
            return False

        # Create a new recording
        if not rec_uuid:
            rec_uuid = str(uuid.uuid4())
        recording = Recording(uuid=rec_uuid)

        # Store the recording
        self.recordings.table[rec_uuid] = recording
        self.recordings.current_recording = recording

        # Send the event
        event_data = RecordEvent(uuid=rec_uuid)
        await self.eventbus.asend(Event("record", event_data))

        return True

    async def stop(self) -> bool:

        # If there was a recording, stop it
        if self.recordings.current_recording:
            self.recordings.current_recording.stop()
            self.recordings.current_recording = None

        # Send the event
        await self.eventbus.asend(Event("stop"))

        return True

    async def collect(self) -> bool:

        # If there was a recording, stop it
        if self.recordings.current_recording:
            self.recordings.current_recording.stop()
            self.recordings.current_recording = None

        # Send the event
        await self.eventbus.asend(Event("collect"))

        return True

    async def session_files_formatting(self, file_transfer_records) -> bool:

        for id, file_record in file_transfer_records.records.items():

            # Unpack the transferred files
            await aioshutil.unpack_archive(file_record.location, self.state.logdir)

            # Construct the destination folder
            import pdb

            pdb.set_trace()

        return True

        # for id, file_record in file_transfer_records.records.items():
        #     # Create a folder for the name
        #     named_dst = dst / name
        #     os.mkdir(named_dst)

        #     # Move all the content inside
        #     for filename, file_meta in filepath_dict.items():

        #         # Extract data
        #         filepath = file_meta["dst_filepath"]

        #         # Wait until filepath is completely written
        #         success = waiting_for(
        #             condition=lambda: filepath.exists(),
        #             timeout=config.get("comms.timeout.zip-time-write"),
        #         )

        #         if not success:
        #             return False

        #         # If not unzip, just move it
        #         if not unzip:
        #             shutil.move(filepath, named_dst / filename)

        #         # Otherwise, unzip, move content to the original folder,
        #         # and delete the zip file
        #         else:
        #             shutil.unpack_archive(filepath, named_dst)

        #             # Handling if temp folder includes a _ in the beginning
        #             new_filename = file_meta["filename"]
        #             if new_filename[0] == "_":
        #                 new_filename = new_filename[1:]
        #             new_filename = new_filename.split(".")[0]

        #             new_file = named_dst / new_filename

        #             # Wait until file is ready
        #             miss_counter = 0
        #             delay = 0.5
        #             timeout = config.get("comms.timeout.zip-time")

        #             while not new_file.exists():
        #                 time.sleep(delay)
        #                 miss_counter += 1
        #                 if timeout < delay * miss_counter:
        #                     self.logger.error(
        #                         f"File zip unpacking took too long! - \
        #                         {name}:{filepath}:{new_file}"
        #                     )
        #                     return False

        #             for file in new_file.iterdir():
        #                 shutil.move(file, named_dst)
        #             shutil.rmtree(new_file)

        # return True
