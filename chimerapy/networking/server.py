# Built-in
from typing import Callable, Dict, Optional, Any, Union, List
import asyncio
import threading
import uuid
import collections
import time
from functools import partial
import pathlib
import tempfile
import shutil
import os
import pickle
import enum

# Third-party
from aiohttp import web, WSCloseCode

# Internal Imports
from chimerapy import config
from .async_loop_thread import AsyncLoopThread
from ..utils import (
    decode_payload,
    create_payload,
    async_waiting_for,
    waiting_for,
    get_ip_address,
)
from .enums import GENERAL_MESSAGE

# Logging
from .. import _logger

logger = _logger.getLogger("chimerapy-networking")

# References
# https://gist.github.com/dmfigol/3e7d5b84a16d076df02baa9f53271058
# https://docs.aiohttp.org/en/stable/web_advanced.html#application-runners
# https://stackoverflow.com/questions/58455058/how-do-i-avoid-the-loop-argument
# https://docs.aiohttp.org/en/stable/web_advanced.html?highlight=weakref#graceful-shutdown
# https://docs.aiohttp.org/en/stable/web_quickstart.html#file-uploads


class Server:
    def __init__(
        self,
        name: str,
        port: int,
        host: str = get_ip_address(),
        routes: Optional[List[web.RouteDef]] = None,
        ws_handlers: Optional[Dict[enum.Enum, Callable]] = None,
    ):
        """Create HTTP Server with WS support.

        Args:
            name (str): Name of the sender.
            port (int): Port for the web server, 0 for random.
            host (Optional[str]): The hosting IP address.
            routes (Optional[Dict[str, Callable]]): HTTP routes.
            ws_handlers (Optional[Dict[enum.Enum, Callable]]): WS routes.

        """
        # Store parameters
        self.name = name
        self.host = host
        self.port = port
        self.routes = routes
        self.ws_handlers = ws_handlers

        # Using flag for marking if system should be running
        self.running = threading.Event()
        self.running.clear()
        self.msg_processed_counter = 0

        # Create AIOHTTP server
        self._app = web.Application()

        # Adding default routes
        self._app.add_routes([web.post("/file/post", self._file_receive)])

        # Creating container for ws clients
        self.ws_clients: Dict[str, Dict[str, Any]] = {}

        # Adding unique routes
        if self.routes:
            self._app.add_routes(self.routes)

        # WS support
        if self.ws_handlers:

            # Adding route for ws and other configuration
            self._app.add_routes([web.get("/ws", self._websocket_handler)])

            # Adding other essential ws handlers
            self.ws_handlers.update(
                {
                    GENERAL_MESSAGE.OK: self._ok,
                    GENERAL_MESSAGE.CLIENT_REGISTER: self._register_ws_client,
                }
            )

        # Adding file transfer capabilities
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())
        self.file_transfer_records = collections.defaultdict(dict)

    def __str__(self):
        return f"<Server {self.name}>"

    ####################################################################
    # Server WS Handlers
    ####################################################################

    async def _ok(self, msg: Dict, ws: web.WebSocketResponse):
        self.uuid_records.append(msg["data"]["uuid"])

    async def _register_ws_client(self, msg: Dict, ws: web.WebSocketResponse):
        # Storing the client information
        self.ws_clients[msg["data"]["client_name"]] = {"ws": ws}

    async def _file_receive(self, request):
        logger.debug(f"{self}: file receive!")

        reader = await request.multipart()

        # /!\ Don't forget to validate your inputs /!\

        # reader.next() will `yield` the fields of your form
        # Get the "meta" field
        field = await reader.next()
        assert field.name == "meta"
        meta_bytes = await field.read(decode=True)
        meta = pickle.loads(meta_bytes)

        # Get the "file" field
        field = await reader.next()
        assert field.name == "file"
        filename = field.filename

        # Create dst filepath
        dst_filepath = self.tempfolder / filename

        # Create the record and mark that is not complete
        # Keep record of the files sent!
        self.file_transfer_records[meta["sender_name"]][filename] = {
            "filename": filename,
            "dst_filepath": dst_filepath,
            "size": 0,
            "complete": False,
        }

        # You cannot rely on Content-Length if transfer is chunked.
        size = 0
        with open(dst_filepath, "wb") as f:
            while True:
                chunk = await field.read_chunk()  # 8192 bytes by default.
                if not chunk:
                    break
                size += len(chunk)
                f.write(chunk)

        # After finishing, mark the size and that is complete
        self.file_transfer_records[meta["sender_name"]][filename].update(
            {"size": size, "complete": True}
        )
        logger.debug(f"Finished updating record: {self.file_transfer_records}")

        return web.Response(text=f"{filename} sized of {size} successfully stored")

    ####################################################################
    # IO Main Methods
    ####################################################################

    async def _read_ws(self, ws: web.WebSocketResponse):
        logger.debug(f"{self}: reading")
        async for aiohttp_msg in ws:

            # Tracking the number of messages processed
            self.msg_processed_counter += 1

            # Extract the binary data and decoded it
            msg = decode_payload(aiohttp_msg.data)
            logger.debug(f"{self}: read - {msg}")

            # Select the handler
            handler = self.ws_handlers[msg["signal"]]
            await handler(msg, ws)

            # Send OK if requested
            if msg["ok"]:
                logger.debug(f"{self}: sending OK for {msg['uuid']}")
                try:
                    await ws.send_bytes(
                        create_payload(GENERAL_MESSAGE.OK, {"uuid": msg["uuid"]})
                    )
                except ConnectionResetError:
                    logger.warning(f"{self}: ConnectionResetError, shutting down ws")
                    await ws.close()
                    return None

    async def _write_ws(self, client_name: str, msg: Dict):
        logger.debug(f"{self}: client_name: {client_name},  write - {msg}")
        ws = self.ws_clients[client_name]["ws"]
        await self._send_msg(ws, **msg)

    async def _websocket_handler(self, request):

        # Register new client
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        # Establish read and write
        read_task = asyncio.create_task(self._read_ws(ws))

        # Continue executing them
        await asyncio.gather(read_task)

        # Close websocket
        await ws.close()

    ####################################################################
    # Server Utilities
    ####################################################################

    async def _send_msg(
        self,
        ws: web.WebSocketResponse,
        signal: enum.Enum,
        data: Dict,
        msg_uuid: str = str(uuid.uuid4()),
        ok: bool = False,
    ):

        # Create payload
        payload = create_payload(signal=signal, data=data, msg_uuid=msg_uuid, ok=ok)

        # Send the message
        try:
            await ws.send_bytes(payload)
        except ConnectionResetError:
            logger.warning(f"{self}: ConnectionResetError, shutting down ws")
            await ws.close()
            return None

        logger.debug(f"{self}: _send_msg -> {signal}")

        # If ok, wait until ok
        if ok:
            success = await async_waiting_for(
                lambda: msg_uuid in self.uuid_records,
                timeout=config.get("comms.timeout.ok"),
            )
            if success:
                logger.debug(f"{self}: receiving OK: SUCCESS")
            else:
                logger.debug(f"{self}: receiving OK: FAILED")

    ####################################################################
    # Server Async Setup and Shutdown
    ####################################################################

    async def _main(self):

        # Create record of message uuids
        self.uuid_records = collections.deque(maxlen=100)

        # Use an application runner to run the web server
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()

        # If port selected 0, then obtain the randomly selected port
        if self.port == 0:
            self.port = self._site._server.sockets[0].getsockname()[1]

        # Create flag to mark that server is ready
        self._server_ready.set()

    async def _server_shutdown(self):

        self.running.clear()

        for client_data in self.ws_clients.values():
            client_ws = client_data["ws"]
            await asyncio.wait_for(
                client_ws.close(
                    code=WSCloseCode.GOING_AWAY, message=f"{self}: shutdown"
                ),
                timeout=2,
            )

        # Cleanup and signal complete
        await asyncio.wait_for(self._runner.shutdown(), timeout=5)
        await asyncio.wait_for(self._runner.cleanup(), timeout=5)
        self._server_shutdown_complete.set()

    ####################################################################
    # Server ASync Lifecycle API
    ####################################################################

    async def async_send(
        self, client_name: str, signal: enum.Enum, data: Dict, ok: bool = False
    ):

        # Create uuid
        msg_uuid = str(uuid.uuid4())

        # Create msg container and execute writing coroutine
        msg = {"signal": signal, "data": data, "msg_uuid": msg_uuid, "ok": ok}
        await self._write_ws(client_name, msg)

        if ok:
            success = await async_waiting_for(
                lambda: msg_uuid in self.uuid_records,
                timeout=config.get("comms.timeout.ok"),
            )
            if success:
                logger.debug(f"{self}: receiving OK: SUCCESS")
            else:
                logger.debug(f"{self}: receiving OK: FAILED")

    async def async_broadcast(self, signal: enum.Enum, data: Dict, ok: bool = False):
        # Create msg container and execute writing coroutine for all
        # clients
        msg = {"signal": signal, "data": data, "ok": ok}
        for client_name in self.ws_clients:
            await self._write_ws(client_name, msg)

    ####################################################################
    # Server Sync Lifecycle API
    ####################################################################

    def serve(self):

        # Create async loop in thread
        self._server_ready = threading.Event()
        self._server_ready.clear()
        self._thread = AsyncLoopThread()
        self._thread.start()

        # Mark that the server is running
        self.running.set()

        # Start aiohttp server
        self._thread.exec(self._main)

        # Wait until server is ready
        flag = self._server_ready.wait(timeout=config.get("comms.timeout.server-ready"))
        if flag == 0:

            logger.debug(f"{self}: failed to start, shutting down!")
            self.shutdown()
            raise TimeoutError(f"{self}: failed to start, shutting down!")

        else:
            logger.debug(f"{self}: running at {self.host}:{self.port}")

    def send(self, client_name: str, signal: enum.Enum, data: Dict, ok: bool = False):

        # Create uuid
        msg_uuid = str(uuid.uuid4())

        # Create msg container and execute writing coroutine
        msg = {"signal": signal, "data": data, "msg_uuid": msg_uuid, "ok": ok}
        self._thread.exec(partial(self._write_ws, client_name, msg))

        if ok:
            success = waiting_for(
                lambda: msg_uuid in self.uuid_records,
                timeout=config.get("comms.timeout.ok"),
            )
            if success:
                logger.debug(f"{self}: receiving OK: SUCCESS")
            else:
                logger.debug(f"{self}: receiving OK: FAILED")

    def broadcast(self, signal: enum.Enum, data: Dict, ok: bool = False):
        # Create msg container and execute writing coroutine for all
        # clients
        msg = {"signal": signal, "data": data, "ok": ok}
        for client_name in self.ws_clients:
            self._thread.exec(partial(self._write_ws, client_name, msg))

    def move_transfer_files(self, dst: pathlib.Path, unzip: bool) -> bool:

        for name, filepath_dict in self.file_transfer_records.items():
            # Create a folder for the name
            named_dst = dst / name
            os.mkdir(named_dst)

            # Move all the content inside
            for filename, file_meta in filepath_dict.items():

                # Extract data
                logger.debug(f"{self}: move_transfer_files, file_meta = {file_meta}")
                filepath = file_meta["dst_filepath"]

                # If not unzip, just move it
                if not unzip:
                    shutil.move(filepath, named_dst / filename)

                # Otherwise, unzip, move content to the original folder,
                # and delete the zip file
                else:
                    shutil.unpack_archive(filepath, named_dst)

                    # Handling if temp folder includes a _ in the beginning
                    new_filename = filepath.stem
                    if new_filename[0] == "_":
                        new_filename = new_filename[1:]

                    new_file = named_dst / new_filename

                    # Wait until file is ready
                    miss_counter = 0
                    delay = 0.5
                    timeout = config.get("comms.timeout.zip-time")

                    while not new_file.exists():
                        time.sleep(delay)
                        miss_counter += 1
                        if timeout < delay * miss_counter:
                            logger.error(
                                f"File zip unpacking took too long! - {name}:{filepath}:{new_file}"
                            )
                            return False

                    for file in new_file.iterdir():
                        shutil.move(file, named_dst)
                    shutil.rmtree(new_file)

        return True

    def shutdown(self):

        # Only shutdown for the first time
        if self.running.is_set():

            # Use client event for shutdown
            self._server_shutdown_complete = threading.Event()
            self._server_shutdown_complete.clear()

            # Execute shutdown
            self._thread.exec(self._server_shutdown)

            # Wait for it
            if not self._server_shutdown_complete.wait(
                timeout=config.get("comms.timeout.server-shutdown")
            ):
                logger.warning(f"{self}: failed to gracefully shutdown")

            # Stop the async thread
            self._thread.stop()
