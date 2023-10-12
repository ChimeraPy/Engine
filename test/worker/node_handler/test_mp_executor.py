import asyncio

# import multiprocess as mp
import multiprocessing as mp
import time
from concurrent.futures import Future

import pytest

import chimerapy.engine as cpe
from chimerapy.engine.worker.node_handler_service.context_session import (
    MultiprocessExecutor,
)

logger = cpe._logger.getLogger("chimerapy-engine")

# Constants
N = 10


@pytest.fixture
def mp_manager():
    return mp.Manager()


def worker(running, counter):

    counter.value += 1
    while running.value:
        counter.value += 1
        time.sleep(0.1)

    return counter.value


def test_instance_mp_manager(mp_manager):
    running = mp_manager.Value("i", 1)
    counter = mp_manager.Value("i", 0)
    assert running.value == 1
    assert counter.value == 0


def test_mp_pool(mp_manager):
    running = mp_manager.Value("i", 1)
    counter = mp_manager.Value("i", 0)

    pool = mp.Pool(processes=1)
    future = Future()
    result = pool.apply_async(
        worker,
        (running, counter),
        callback=future.set_result,
        error_callback=future.set_exception,
    )
    future._result = result
    time.sleep(2)
    running.value = 0
    final_counter = future.result()
    # logger.debug(f"final_counter = {final_counter}")
    assert final_counter != 0
    pool.close()
    pool.join()


async def test_mp_executor(mp_manager):
    running = mp_manager.Value("i", 1)
    counter = mp_manager.Value("i", 0)

    loop = asyncio.get_running_loop()
    pool = mp.Pool(processes=1)
    executor = MultiprocessExecutor(pool)

    future = loop.run_in_executor(executor, worker, running, counter)
    await asyncio.sleep(2)

    running.value = 0
    final_counter = await future
    assert final_counter != 0
