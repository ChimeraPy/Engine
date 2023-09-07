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

    def callback(self, coro: Coroutine) -> Tuple[Future, Coroutine]:
        
        future = Future()
        
        async def _wrapper():
            output = None
            try:
                output = await coro
            except KeyboardInterrupt:
                logger.debug("KeyboardInterrupt DETECTED")
                future.set_result(None)
                self.stop()
                return None
            except Exception:
                logger.error(traceback.format_exc())
                
            future.set_result(output)

        return future, _wrapper()
    
    def waitable_callback(
        self, callback: Callable, args: List[Any]
    ) -> Tuple[Future, Callable]:

        future = Future()

        # Create wrapper that signals when the callback finished
        def _wrapper(func: Callable, *args) -> Any:
            output = None
            try:
                output = func(*args)
            except KeyboardInterrupt:
                logger.debug("KeyboardInterrupt DETECTED")
                future.set_result(None)
                self.stop()
                return None
            except Exception as e:
                logger.error(traceback.format_exc())

            future.set_result(output)
            return output

        wrapper = _wrapper(callback, *args)
        return future, wrapper

    def exec(self, coro: Coroutine) -> threading.Event:
        finished, wrapper = self.callback(coro)
        asyncio.run_coroutine_threadsafe(wrapper, self._loop)
        return finished
    
    def exec_noncoro(
        self, callback: Callable, args: List[Any], waitable: bool = False
    ) -> Optional[threading.Event]:

        if waitable:
            finished, wrapper = self.waitable_callback(callback, args)
            self._loop.call_soon_threadsafe(wrapper, *args)
            return finished

        else:
            self._loop.call_soon_threadsafe(callback, *args)

        return None

    def run(self):
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_forever()
        except KeyboardInterrupt:
            ...
        finally:
            self._loop.close()
            
    def flush(self, timeout: Optional[Union[int, float]] = None): 
        tasks = asyncio.all_tasks(self._loop)
        if tasks:
            coro = asyncio.gather(*tasks)
            self.exec(coro).result(timeout=timeout)

    def stop(self):
        # Cancel all tasks
        for task in asyncio.all_tasks(self._loop):
            task.cancel()
        self._loop.stop()

    def __del__(self):
        self.stop()
