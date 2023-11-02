import abc
import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
from functools import partial
from typing import Awaitable, Callable, List, Optional, Union

import multiprocess as mp


class MultiprocessExecutor:
    def __init__(self, pool: Optional[mp.Pool] = None, processes=None):
        if pool is not None:
            self.pool = pool
        else:
            self.pool = mp.Pool(processes)

    def submit(self, fn, *args, **kwargs):
        future = Future()
        result = self.pool.apply_async(
            fn,
            args,
            kwargs,
            callback=future.set_result,
            error_callback=future.set_exception,
        )
        future._result = result  # Store this to prevent it from being garbage-collected
        return future

    def shutdown(self, wait=True):
        self.pool.close()
        if wait:
            self.pool.join()


class ContextSession(abc.ABC):

    futures: List[Awaitable]
    loop: asyncio.AbstractEventLoop
    pool: Optional[mp.Pool]
    executor: Union[MultiprocessExecutor, ThreadPoolExecutor]

    async def wait_for_all(self):
        await asyncio.wait(self.futures)

    def add(self, f: Callable, *args, **kwargs) -> Awaitable:
        ret = self.loop.run_in_executor(self.executor, partial(f, *args, **kwargs))
        self.futures.append(ret)
        return ret

    def shutdown(self):
        self.executor.shutdown()


class MPSession(ContextSession):
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.pool = mp.Pool()
        self.executor = MultiprocessExecutor(self.pool)
        self.futures = []


class ThreadSession(ContextSession):
    def __init__(self):
        self.loop = asyncio.get_running_loop()
        self.pool = None
        self.executor = ThreadPoolExecutor()
        self.futures = []
