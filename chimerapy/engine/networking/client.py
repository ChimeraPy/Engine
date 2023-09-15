# Built-in
import os
import asyncio
import threading
import collections
import uuid
import pathlib
import aioshutil
import tempfile
import json
import enum
import logging
import traceback
import asyncio_atexit
from concurrent.futures import Future
from typing import Dict, Optional, Callable, Any, Union, List, Coroutine

# Third-party
import aiohttp

# Internal Imports
from chimerapy.engine import config
from ..utils import create_payload, async_waiting_for
from .async_loop_thread import AsyncLoopThread
from .enums import GENERAL_MESSAGE

# Logging
from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy-engine-networking")

# References
# https://gist.github.com/dmfigol/3e7d5b84a16d076df02baa9f53271058
# https://docs.aiohttp.org/en/stable/web_advanced.html#application-runners
# https://docs.aiohttp.org/en/stable/client_reference.html?highlight=websocket%20close%20open#clientwebsocketresponse


class Client:
    def __init__(
        self,
        id: str,
        host: str,
        port: int,
        ws_handlers: Dict[enum.Enum, Callable] = {},
        thread: Optional[AsyncLoopThread] = None,
        parent_logger: Optional[logging.Logger] = None,
    ):

        # Store parameters
        self.id = id
        self.host = host
        self.port = port
        self.ws_handlers = {k.value: v for k, v in ws_handlers.items()}
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._session = None

        # The EventLoop
        self._thread = thread
        self.futures: List[Future] = []

        # State variables
        self.running = threading.Event()
        self.running.clear()
        self.msg_processed_counter = 0
        self.uuid_records: collections.deque[str] = collections.deque(maxlen=100)
        self.tasks: List[asyncio.Task] = []

        # Adding default client handlers
        self.ws_handlers.update(
            {
                GENERAL_MESSAGE.OK.value: self._ok,
                GENERAL_MESSAGE.SHUTDOWN.value: self.async_shutdown,
            }
        )

        # Adding file transfer capabilities
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())

        if parent_logger is not None:
            self.logger = _logger.fork(parent_logger, "client")
        else:
            self.logger = _logger.getLogger("chimerapy-engine-networking")

    def __str__(self):
        return f"<Client {self.id}>"

    def setLogger(self, parent_logger: logging.Logger):
        self.logger = _logger.fork(parent_logger, "client")

    ####################################################################
    # Helper Function
    ####################################################################

    def _exec_coro(self, coro: Coroutine) -> Future:
        assert self._thread
        # Submitting the coroutine
        future = self._thread.exec(coro)

        # Saving the future for later use
        self.futures.append(future)

        return future

    ####################################################################
    # Client WS Handlers
    ####################################################################

    async def _ok(self, msg: Dict):
        # self.logger.debug(f"{self}: received OK")
        self.uuid_records.append(msg["data"]["uuid"])

    ####################################################################
    # IO Main Methods
    ####################################################################

    async def _read_ws(self):
        assert self._ws

        async for aiohttp_msg in self._ws:

            # self.logger.debug(f"{self}: msg: {aiohttp_msg}")

            # Tracking the number of messages processed
            self.msg_processed_counter += 1

            # Extract the binary data and decoded it
            msg = aiohttp_msg.json()

            # Select the handler
            handler = self.ws_handlers[msg["signal"]]
            await handler(msg)

            # Send OK if requested
            if msg["ok"]:
                try:
                    await self._ws.send_json(
                        create_payload(GENERAL_MESSAGE.OK, {"uuid": msg["uuid"]})
                    )
                except ConnectionResetError:
                    # self.logger.warning(
                    #     f"{self}: ConnectionResetError, shutting down ws"
                    # )
                    await self._ws.close()
                    return None

    async def _send_msg(
        self,
        signal: enum.Enum,
        data: Dict,
        msg_uuid: str = str(uuid.uuid4()),
        ok: bool = False,
    ):
        assert self._ws

        # Handle if closed
        if self._ws.closed:
            return None

        # Create payload
        payload = create_payload(signal=signal, data=data, msg_uuid=msg_uuid, ok=ok)

        # Send the message
        try:
            await self._ws.send_json(payload)
        except ConnectionResetError:
            # self.logger.warning(f"{self}: ConnectionResetError, shutting down ws")
            await self._ws.close()
            return None

        # If ok, wait until ok
        if ok:

            await async_waiting_for(
                lambda: msg_uuid in self.uuid_records,
                timeout=config.get("comms.timeout.ok"),
            )

    async def _register(self):

        # First message should be the client registering to the Server
        await self._send_msg(
            signal=GENERAL_MESSAGE.CLIENT_REGISTER,
            data={"client_id": self.id},
            ok=True,
        )

        # self.logger.debug(f"{self}: registered!")

    ####################################################################
    # Client Async Setup and Shutdown
    ####################################################################

    async def async_connect(self) -> bool:

        # Reset
        self.uuid_records.clear()

        # Create the session
        self._session = aiohttp.ClientSession()
        assert self._session
        self._ws = await self._session.ws_connect(f"http://{self.host}:{self.port}/ws")

        # Create task to read
        task = asyncio.create_task(self._read_ws())
        self.tasks.append(task)

        # Register the client
        await self._register()

        # Make sure to shutdown correctly
        asyncio_atexit.register(self.async_shutdown)

        return True

    async def async_send_file(
        self, url: str, sender_id: str, filepath: pathlib.Path
    ) -> bool:

        # Make a post request to send the file
        data = aiohttp.FormData()
        data.add_field(
            "meta",
            json.dumps({"sender_id": sender_id, "size": os.path.getsize(filepath)}),
            content_type="application/json",
        )
        data.add_field(
            "file",
            open(filepath, "rb"),
            filename=filepath.name,
            content_type="application/zip",
        )

        # Create a new session for the moment
        async with aiohttp.ClientSession() as session:
            await session.post(url, data=data)

        return True

    async def async_send_folder(self, sender_id: str, dir: pathlib.Path) -> bool:

        if not dir.is_dir() and not dir.exists():
            self.logger.error(f"Cannot send non-existent dir: {dir}.")
            return False

        zip_file = dir.parent / f"{dir.name}.zip"
        try:
            await aioshutil.make_archive(str(dir), "zip", dir.parent, dir.name)
        except Exception:
            self.logger.warning(f"{self}: Temp folder couldn't be zipped.")
            self.logger.error(traceback.format_exc())
            return False

        # Compose the url
        url = f"http://{self.host}:{self.port}/file/post"

        # Then send the file
        return await self.async_send_file(url, sender_id, zip_file)

    async def async_shutdown(self, msg: Dict = {}):

        if self._ws:
            await asyncio.wait_for(self._ws.close(), timeout=5)
        if self._session:
            await asyncio.wait_for(self._session.close(), timeout=5)

    async def async_send(self, signal: enum.Enum, data: Any, ok: bool = False) -> bool:

        # Create uuid
        msg_uuid = str(uuid.uuid4())

        # Create msg container and execute writing coroutine
        msg = {"signal": signal, "data": data, "msg_uuid": msg_uuid, "ok": ok}
        await self._send_msg(**msg)

        if ok:

            success = await async_waiting_for(
                lambda: msg_uuid in self.uuid_records,
                timeout=config.get("comms.timeout.ok"),
            )
            if not success:
                self.logger.warning(f"{self}: Timeout in OK")

        return True

    ####################################################################
    # Client Sync Lifecyle API
    ####################################################################

    def send(self, signal: enum.Enum, data: Any, ok: bool = False) -> Future[bool]:
        return self._exec_coro(self.async_send(signal, data, ok))

    def send_file(self, sender_id: str, filepath: pathlib.Path) -> Future[bool]:
        # Compose the url
        url = f"http://{self.host}:{self.port}/file/post"
        return self._exec_coro(self.async_send_file(url, sender_id, filepath))

    def send_folder(self, sender_id: str, dir: pathlib.Path) -> Future[bool]:

        assert (
            dir.is_dir() and dir.exists()
        ), f"Sending {dir} needs to be a folder that exists."

        return self._exec_coro(self.async_send_folder(sender_id, dir))

    def connect(self, blocking: bool = True) -> Union[bool, Future[bool]]:

        if not self._thread:
            self._thread = AsyncLoopThread()
            self._thread.start()
        else:
            if not self._thread.is_alive():
                self._thread.start()

        future = self._exec_coro(self.async_connect())

        if blocking:
            return future.result(timeout=config.get("comms.timeout.client-ready"))

        return future

    def shutdown(self, blocking: bool = True) -> Union[bool, Future[bool]]:
        future = self._exec_coro(self.async_shutdown())

        if blocking:
            return future.result(timeout=config.get("comms.timeout.client-shutdown"))

        return future
