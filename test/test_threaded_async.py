# Built-in Imports
import time
import asyncio

# Third-party Imports
import pytest

# ChimeraPy Imports
from chimerapy.networking import AsyncLoopThread
import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
cp.debug()


@pytest.fixture
def thread():
    thread = AsyncLoopThread()
    thread.start()
    yield thread
    thread.stop()


def test_callback_execution(thread):
    queue = asyncio.Queue()

    def put(queue):
        logger.debug(f"put called")
        queue.put_nowait(1)

    thread.exec_noncoro(put, args=[queue])
    time.sleep(1)
    assert queue.qsize() == 1


def test_callback_execution_with_wait(thread):
    queue = asyncio.Queue()

    def put(queue):
        logger.debug(f"put called")
        queue.put_nowait(1)

    finished = thread.exec_noncoro(put, args=[queue], waitable=True)
    assert finished.wait(timeout=1)
    assert queue.qsize() == 1
