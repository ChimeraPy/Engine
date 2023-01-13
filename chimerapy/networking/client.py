# Built-in
from typing import Coroutine, Dict, Optional, Callable
import asyncio
import threading

# Third-party
import aiohttp
from aiohttp import web

# Internal Imports
from .async_loop_thread import AsyncLoopThread
from .utils import create_payload, decode_payload
from .enums import CLIENT_MESSAGE

# Logging
from .. import _logger

logger = _logger.getLogger("chimerapy-networking")

# References
# https://gist.github.com/dmfigol/3e7d5b84a16d076df02baa9f53271058
# https://docs.aiohttp.org/en/stable/web_advanced.html#application-runners
# https://docs.aiohttp.org/en/stable/client_reference.html?highlight=websocket%20close%20open#clientwebsocketresponse


class Client:
    def __init__(
        self,
        name: str,
        host: str,
        port: int,
        ws_handlers=Dict[str, Callable[[], Coroutine]],
    ):

        # Store parameters
        self.name = name
        self.host = host
        self.port = port
        self.ws_handlers = ws_handlers

        # Create session
        self._session = aiohttp.ClientSession()
        self._send_msg_queue = asyncio.Queue()

    def __str__(self):
        return f"<Client {self.name}>"

    async def _read_ws(self, ws):
        logger.debug(f"{self}: reading")
        async for msg in ws:
            logger.debug(f"{self}: read ws - {msg}")

    async def _write_ws(self, ws):
        logger.debug(f"{self}: writing")

        # Then the other messages can be made by the queue
        while True:
            msg = await self._send_msg_queue.get()
            await ws.send_bytes(create_payload(**msg))

    async def _main(self):

        logger.debug(f"{self}: main -> http://{self.host}:{self.port}/ws")

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f"http://{self.host}:{self.port}/ws") as ws:

                # First message should be the client registering to the Server
                await ws.send_bytes(
                    create_payload(
                        signal=CLIENT_MESSAGE.REGISTER, data={"client_name": self.name}
                    )
                )

                # Mark that client has connected
                self._client_ready.set()

                # Establish read and write
                read_task = asyncio.create_task(self._read_ws(ws))
                write_task = asyncio.create_task(self._write_ws(ws))

                # Continue executing them
                await asyncio.gather(read_task, write_task)

                # Then close the socket
                await ws.close()

    async def _client_shutdown(self):
        await self._session.close()

    def send(self, msg):
        self._thread.exec_noncoro(self._send_msg_queue.put_nowait, msg)

    def connect(self):

        # Create async loop in thread
        self._client_ready = threading.Event()
        self._client_ready.clear()
        self._thread = AsyncLoopThread()
        self._thread.start()

        # Start async execution
        self._thread.exec(self._main)

        # Wait until client is ready
        flag = self._client_ready.wait(timeout=10)
        if flag == 0:
            logger.debug(f"{self}: failed to connect, shutting down!")
            self.shutdown()
            raise TimeoutError(f"{self}: failed to connect, shutting down!")
        else:
            logger.debug(f"{self}: connected to {self.host}:{self.port}")

    def shutdown(self):

        # Stop threaded loop
        self._thread.stop()
