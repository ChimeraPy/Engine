# Built-in Imports
import time
import asyncio

# Third-party Imports
import pytest
from pytest import raises

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
        await asyncio.sleep(0.1)
        logger.debug("AFTER SLEEP")
        await queue.put(1)
        logger.debug("FINISHED PUT")

    future = thread.exec(put(queue))
    future.result(timeout=3)
    assert queue.qsize() == 1


def test_callback_execution(thread):
    queue = asyncio.Queue()

    def put(queue):
        logger.debug("put called")
        queue.put_nowait(1)

    future = thread.exec_noncoro(put, args=[queue])
    future.result(timeout=1)
    assert queue.qsize() == 1


def test_callback_execution_with_wait(thread):
    queue = asyncio.Queue()

    def put(queue):
        time.sleep(0.1)
        logger.debug("put called")
        queue.put_nowait(1)

    future = thread.exec_noncoro(put, args=[queue])
    future.result(timeout=1)
    assert queue.qsize() == 1


def test_keyboard_interrupt_handling_noncoro(thread):
    queue = asyncio.Queue()

    # Let's simulate a KeyboardInterrupt using threading after a short delay.
    def raise_keyboard_interrupt(queue):
        time.sleep(0.1)
        queue.put_nowait(1)
        raise KeyboardInterrupt

    future = thread.exec_noncoro(raise_keyboard_interrupt, args=[queue])

    with raises(KeyboardInterrupt):
        future.result(timeout=1)

    assert queue.qsize() == 1
    thread.join()
    assert thread._loop.is_closed()


def test_keyboard_interrupt_handling_coro(thread):
    queue = asyncio.Queue()

    # Let's simulate a KeyboardInterrupt using threading after a short delay.
    async def raise_keyboard_interrupt(queue):
        await asyncio.sleep(0.1)
        await queue.put(1)
        raise KeyboardInterrupt

    future = thread.exec(raise_keyboard_interrupt(queue))
    with raises(KeyboardInterrupt):
        future.result()
    assert queue.qsize() == 1

    thread.join()
    assert thread._loop.is_closed()


def test_exception_handling_noncoro(thread):
    queue = asyncio.Queue()

    # Let's simulate a KeyboardInterrupt using threading after a short delay.
    def raise_keyboard_interrupt(queue):
        time.sleep(0.1)
        queue.put_nowait(1)
        raise TypeError

    future = thread.exec_noncoro(raise_keyboard_interrupt, args=[queue])

    with raises(TypeError):
        future.result(timeout=1)

    assert queue.qsize() == 1
    thread.join()
    assert thread._loop.is_closed()

    with raises(RuntimeError):
        future = thread.exec_noncoro(raise_keyboard_interrupt, args=[queue])


def test_exception_handling_coro(thread):
    queue = asyncio.Queue()

    # Let's simulate a KeyboardInterrupt using threading after a short delay.
    async def raise_keyboard_interrupt(queue):
        await asyncio.sleep(0.1)
        await queue.put(1)
        raise TypeError

    future = thread.exec(raise_keyboard_interrupt(queue))

    with raises(TypeError):
        future.result(timeout=1)

    assert queue.qsize() == 1
    thread.join()
    assert thread._loop.is_closed()

    with raises(RuntimeError):
        future = thread.exec(raise_keyboard_interrupt(queue))
