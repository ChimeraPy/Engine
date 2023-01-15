# Built-in
from typing import Coroutine, Dict, Optional, Callable, Any, Union
import asyncio
import threading
import collections
import uuid
import time
from functools import partial

# Third-party
import aiohttp
from aiohttp import web

# Internal Imports
from .async_loop_thread import AsyncLoopThread
from .utils import create_payload, decode_payload
from .enums import CLIENT_MESSAGE, GENERAL_MESSAGE

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
        ws_handlers=Dict[int, Callable[[], Coroutine]],
    ):

        # Store parameters
        self.name = name
        self.host = host
        self.port = port
        self.ws_handlers = ws_handlers

        # State variables
        self.running = threading.Event()
        self.running.clear()
        self.msg_processed_counter = 0

        # Communication between Async + Sync
        self._send_msg_queue = asyncio.Queue()

        # Adding default client handlers
        self.ws_handlers.update({GENERAL_MESSAGE.OK: self._ok})

    def __str__(self):
        return f"<Client {self.name}>"

    ####################################################################
    # Client WS Handlers
    ####################################################################

    async def _ok(self, msg: Dict, ws: aiohttp.ClientWebSocketResponse):
        self.uuid_records.append(msg["data"]["uuid"])

    ####################################################################
    # IO Main Methods
    ####################################################################

    async def _read_ws(self):
        logger.debug(f"{self}: reading")
        async for aiohttp_msg in self._ws:

            # Tracking the number of messages processed
            self.msg_processed_counter += 1

            # Extract the binary data and decoded it
            msg = decode_payload(aiohttp_msg.data)
            logger.debug(f"{self}: read - {msg}")

            # Select the handler
            handler = self.ws_handlers[msg["signal"]]
            await handler(msg, self._ws)

            # Send OK if requested
            if msg["ok"]:
                logger.debug(f"{self}: sending OK")
                await self._ws.send_bytes(
                    create_payload(GENERAL_MESSAGE.OK, {"uuid": msg["uuid"]})
                )

    async def _write_ws(self, msg: Dict):
        logger.debug(f"{self}: writing - {msg}")
        await self._send_msg(**msg)

    ####################################################################
    # Client Utilities
    ####################################################################

    async def _send_msg(self, signal: int, data: Dict, ok: bool = False):

        # Create uuid
        msg_uuid = str(uuid.uuid4())

        # Create payload
        payload = create_payload(signal=signal, data=data, msg_uuid=msg_uuid, ok=ok)

        # Send the message
        logger.debug(f"{self}: send_msg -> {signal} with OK={ok}")
        await self._ws.send_bytes(payload)

        # If ok, wait until ok
        if ok:
            for i in range(10):
                logger.debug(f"{self}: waiting OK for {msg_uuid}, {self.uuid_records}")
                if msg_uuid in self.uuid_records:
                    logger.debug(f"{self}: OK received")
                    return
                else:
                    await asyncio.sleep(1)

            logger.error(f"{self}: OK was not received!")

    ####################################################################
    # Client Async Setup and Shutdown
    ####################################################################

    async def _register(self):

        # First message should be the client registering to the Server
        await self._send_msg(
            signal=CLIENT_MESSAGE.REGISTER, data={"client_name": self.name}, ok=True
        )

        # Mark that client has connected
        self._client_ready.set()

    async def _main(self):

        logger.debug(f"{self}: main -> http://{self.host}:{self.port}/ws")

        # Create record of message uuids
        self.uuid_records = collections.deque(maxlen=100)

        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(f"http://{self.host}:{self.port}/ws") as ws:

                # Store the Client session
                self._ws = ws

                # Establish read and write
                read_task = asyncio.create_task(self._read_ws())

                # Register the client
                await self._register()

                # Continue executing them
                await asyncio.gather(read_task)

                # Then close the socket
                await ws.close()

    async def _client_shutdown(self):

        # Mark to stop and close things
        self.running.clear()
        await self._ws.close()

        del self._ws

        self._client_shutdown_complete.set()

    ####################################################################
    # Client Sync Lifecyle API
    ####################################################################

    def send(self, signal: str, data: Any, ok: bool = False):

        # Create msg container and execute writing coroutine
        msg = {"signal": signal, "data": data, "ok": ok}
        self._thread.exec(partial(self._write_ws, msg))

    def connect(self):

        # Mark that the client is running
        self.running.set()

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
            self.shutdown()
            raise TimeoutError(f"{self}: failed to connect, shutting down!")
        else:
            logger.debug(f"{self}: connected to {self.host}:{self.port}")

    def shutdown(self):

        # Use client event for shutdown
        self._client_shutdown_complete = threading.Event()
        self._client_shutdown_complete.clear()

        # Execute shutdown
        self._thread.exec(self._client_shutdown)

        # Wait for it
        if not self._client_shutdown_complete.wait(timeout=5):
            logger.warning(f"{self}: failed to gracefully shutdown")

        # Stop threaded loop
        self._thread.stop()
