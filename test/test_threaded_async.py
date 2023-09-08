# Built-in Imports
import time
import asyncio
import threading

# Third-party Imports
import pytest

# ChimeraPy Imports
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
import chimerapy.engine as cpe

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

    future = thread.exec(put(queue))
    future.result(timeout=5)
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

    future = thread.exec_noncoro(put, args=[queue], waitable=True)
    future.result(timeout=5)
    assert queue.qsize() == 1
    

def test_keyboard_interrupt_handling_noncoro(thread):
    queue = asyncio.Queue()
    
    # Let's simulate a KeyboardInterrupt using threading after a short delay.
    def raise_keyboard_interrupt(queue):
        time.sleep(1)
        queue.put_nowait(1)
        raise KeyboardInterrupt
    
    future = thread.exec_noncoro(raise_keyboard_interrupt, args=[queue], waitable=True)
    future.result(timeout=5)
    assert queue.qsize() == 1
    thread.join()
    assert thread._loop.is_closed()
    
    
def test_keyboard_interrupt_handling_coro(thread):
    queue = asyncio.Queue()
    
    # Let's simulate a KeyboardInterrupt using threading after a short delay.
    async def raise_keyboard_interrupt(queue):
        await asyncio.sleep(1)
        await queue.put(1)
        raise KeyboardInterrupt
    
    future = thread.exec(raise_keyboard_interrupt(queue))
    future.result(timeout=5)
    assert queue.qsize() == 1
    
    thread.join()
    assert thread._loop.is_closed()
    
