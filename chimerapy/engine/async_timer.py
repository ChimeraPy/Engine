import asyncio


class AsyncTimer:
    def __init__(self, callback, interval):
        self._callback = callback
        self._interval = interval
        self._is_running = False
        self._task = None

    async def _run(self):
        while self._is_running:
            await self._callback()
            await asyncio.sleep(self._interval)

    async def start(self):
        if not self._is_running:
            self._is_running = True
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        if self._is_running:
            self._is_running = False
            self._task.cancel()
