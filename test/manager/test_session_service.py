import logging

import pytest

from chimerapy.engine.manager.session_record_service import SessionRecordService
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.networking.server import FileTransferTable, FileTransferRecord
from chimerapy.engine.eventbus import EventBus, Event
from chimerapy.engine.states import ManagerState

from ..conftest import TEST_SAMPLE_DATA_DIR, TEST_DATA_DIR

logger = logging.getLogger("chimerapy-engine")


@pytest.fixture
def session_service():

    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    state = ManagerState(logdir=TEST_DATA_DIR)

    session_service = SessionRecordService("session_record", eventbus, state)

    eventbus.send(Event("start"))
    yield session_service
    eventbus.send(Event("stop"))


@pytest.fixture
def file_transfer_records():

    file_transfer_records = FileTransferTable()

    for name in ["test1.zip", "test2.zip", "test3.zip"]:
        file_entry = FileTransferRecord(
            sender_id="test",
            filename=name,
            uuid=name,
            location=TEST_SAMPLE_DATA_DIR / "session_service_mock" / name,
            size=-1,
        )
        file_transfer_records.records[name] = file_entry

    return file_transfer_records


def test_instanciate(session_service):
    ...


@pytest.mark.asyncio
async def test_session_recording(session_service):

    await session_service.record()
    await session_service.stop()
    await session_service.collect()


@pytest.mark.asyncio
async def test_session_files(session_service, file_transfer_records):

    for name in ["uuid1", "uuid2", "uuid3"]:
        await session_service.record(name)
        await session_service.stop()

    await session_service.session_files_formatting(file_transfer_records)
