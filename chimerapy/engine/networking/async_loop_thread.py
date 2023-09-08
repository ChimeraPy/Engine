# Built-in
from typing import Coroutine, Callable, Tuple, List, Optional, Any, Union
import threading
import asyncio
import traceback
from concurrent.futures import Future

# Internal Imports
from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy-engine")

# Reference
# https://stackoverflow.com/a/66055205/13231446


# first, we need a loop running in a parallel Thread
class AsyncLoopThread(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self._loop = asyncio.new_event_loop()

    def callback(
        self, func: Union[Callable, Coroutine], args: Optional[List[Any]] = None
    ) -> Tuple[Future, Coroutine]:

        future: Future = Future()

        if args is None:
            args = []

        # Create wrapper that signals when the callback finished
        async def _wrapper(func: Union[Callable, Coroutine], *args) -> Any:
            try:
                if asyncio.iscoroutine(func):
                    result = await func  # type: ignore
                else:
                    result = func(*args)  # type: ignore
                future.set_result(result)
                return result
            except (KeyboardInterrupt, Exception) as e:
                if isinstance(e, KeyboardInterrupt):
                    logger.debug("KeyboardInterrupt DETECTED")
                else:
                    logger.error(traceback.format_exc())
                future.set_exception(e)
                self.stop()
                return None

        wrapper = _wrapper(func, *args)
        return future, wrapper

    def exec(self, coro: Coroutine) -> Future:
        if self._loop.is_closed():
            raise RuntimeError(
                "AsyncLoopThread: Event loop is closed, but a coroutine was sent to it."
            )

        finished, wrapper = self.callback(coro)
        asyncio.run_coroutine_threadsafe(wrapper, self._loop)
        return finished

    def exec_noncoro(self, callback: Callable, args: List[Any]) -> Future:
        if self._loop.is_closed():
            raise RuntimeError(
                "AsyncLoopThread: Event loop is closed, but a coroutine was sent to it."
            )

        finished, wrapper = self.callback(callback, args)
        asyncio.run_coroutine_threadsafe(wrapper, self._loop)
        return finished

    def run(self):
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            ...
        finally:
            self.stop()
            self._loop.close()

    def flush(self, timeout: Optional[Union[int, float]] = None):
        tasks = asyncio.all_tasks(self._loop)
        if tasks:
            coro = asyncio.gather(*tasks)
            self.exec(coro).result(timeout=timeout)  # type: ignore

    def stop(self):
        # Cancel all tasks
        for task in asyncio.all_tasks(self._loop):
            task.cancel()
        self._loop.stop()

    def __del__(self):
        self.stop()
