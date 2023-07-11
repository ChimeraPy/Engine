# Built-in Imports
import time
import asyncio

# Third-party Imports
import pytest

# ChimeraPy Imports
from chimerapy_engine.networking.async_loop_thread import AsyncLoopThread
import chimerapy_engine as cpe

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


@pytest.fixture
def thread():
    thread = AsyncLoopThread()
    thread.start()
    yield thread
    thread.stop()


def test_coroutine_waitable_execution(thread):
    queue = asyncio.Queue()

    async def put(queue):
        logger.debug("PUT")
        await asyncio.sleep(1)
        logger.debug("AFTER SLEEP")
        await queue.put(1)
        logger.debug("FINISHED PUT")

    finished = thread.exec(put(queue))
    finished.result(timeout=5)
    assert queue.qsize() == 1


def test_callback_execution(thread):
    queue = asyncio.Queue()

    def put(queue):
        logger.debug("put called")
        queue.put_nowait(1)

    thread.exec_noncoro(put, args=[queue])
    time.sleep(5)
    assert queue.qsize() == 1


def test_callback_execution_with_wait(thread):
    queue = asyncio.Queue()

    def put(queue):
        time.sleep(1)
        logger.debug("put called")
        queue.put_nowait(1)

    finished = thread.exec_noncoro(put, args=[queue], waitable=True)
    assert finished.wait(timeout=5)
    assert queue.qsize() == 1
