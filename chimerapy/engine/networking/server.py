# Built-in
from typing import Callable, Dict, Optional, Any, List, Coroutine
import asyncio
import logging
import uuid
import collections
import time
import pathlib
import tempfile
import shutil
import os
import json
import enum
import traceback
import aiofiles
import atexit
from concurrent.futures import Future

# Third-party
from tqdm import tqdm
from aiohttp import web

# Internal Imports
from chimerapy.engine import config
from .async_loop_thread import AsyncLoopThread
from chimerapy.engine.utils import (
    create_payload,
    async_waiting_for,
    waiting_for,
    get_ip_address,
)
from .enums import GENERAL_MESSAGE

# Logging
from chimerapy.engine import _logger

# logger = _self.logger.getLogger("chimerapy-networking")

# References
# https://gist.github.com/dmfigol/3e7d5b84a16d076df02baa9f53271058
# https://docs.aiohttp.org/en/stable/web_advanced.html#application-runners
# https://stackoverflow.com/questions/58455058/how-do-i-avoid-the-loop-argument
# https://docs.aiohttp.org/en/stable/web_advanced.html?highlight=weakref#graceful-shutdown
# https://docs.aiohttp.org/en/stable/web_quickstart.html#file-uploads


class Server:
    def __init__(
        self,
        id: str,
        port: int,
        host: str = get_ip_address(),
        routes: List[web.RouteDef] = [],
        ws_handlers: Dict[enum.Enum, Callable] = {},
        thread: Optional[AsyncLoopThread] = None,
        parent_logger: Optional[logging.Logger] = None,
    ):
        """Create HTTP Server with WS support.

        Args:
            id (str): ID of the sender.
            port (int): Port for the web server, 0 for random.
            host (Optional[str]): The hosting IP address.
            routes (Optional[Dict[str, Callable]]): HTTP routes.
            ws_handlers (Optional[Dict[enum.Enum, Callable]]): WS routes.

        """
        # Store parameters
        self.id = id
        self.host = host
        self.port = port
        self.routes = routes
        self.ws_handlers = {k.value: v for k, v in ws_handlers.items()}

        # The EventLoop
        if thread:
            self._thread = thread
        else:
            self._thread = AsyncLoopThread()
            self._thread.start()

        self.futures: List[Future] = []

        # Using flag for marking if system should be running
        self.running: bool = False
        self.msg_processed_counter = 0

        # Create AIOHTTP server
        self._app = web.Application(middlewares=[self.logging_middleware])

        # Adding default routes
        self._app.add_routes([web.post("/file/post", self._file_receive)])

        # Creating container for ws clients
        self.ws_clients: Dict[str, web.WebSocketResponse] = {}

        # Adding unique routes
        if self.routes:
            self._app.add_routes(self.routes)

        # Adding route for ws and other configuration
        self._app.add_routes([web.get("/ws", self._websocket_handler)])

        # Adding other essential ws handlers
        self.ws_handlers.update(
            {
                GENERAL_MESSAGE.OK.value: self._ok,
                GENERAL_MESSAGE.CLIENT_REGISTER.value: self._register_ws_client,
            }
        )

        # Adding file transfer capabilities
        self.tempfolder = pathlib.Path(tempfile.mkdtemp())
        self.file_transfer_records: Dict[
            str, Dict[str, Dict[str, Any]]
        ] = collections.defaultdict(
            dict
        )  # Need to refactor this!

        if parent_logger is not None:
            self.logger = _logger.fork(parent_logger, "server")
        else:
            self.logger = _logger.getLogger("chimerapy-engine-networking")

        # Make sure to shutdown correctly
        atexit.register(self.shutdown)

    def __str__(self):
        return f"<Server {self.id}>"

    ####################################################################
    # Helper Function
    ####################################################################

    def _exec_coro(self, coro: Coroutine) -> Future:
        # Submitting the coroutine
        future = self._thread.exec(coro)

        # Saving the future for later use
        self.futures.append(future)

        return future

    ####################################################################
    # Middleware
    ####################################################################

    async def logging_middleware(self, app, handler):
        async def middleware_handler(request):
            # Log the incoming request
            # print(f"Received request: {request.method} {request.path}")

            # Process the request
            response = await handler(request)
            self.logger.info(f"{request.method} {request.path} - {response.status}")

            # Log the response (you can enhance this to provide more detailed logging)
            # print(f"Response: {response.status} - {response.reason}")
            return response

        return middleware_handler

    ####################################################################
    # Server Setters and Getters
    ####################################################################

    def add_routes(self, routes: List[web.RouteDef]):
        self._app.add_routes(routes)

    ####################################################################
    # Server WS Handlers
    ####################################################################

    async def _ok(self, msg: Dict, ws: web.WebSocketResponse):
        self.uuid_records.append(msg["data"]["uuid"])

    async def _register_ws_client(self, msg: Dict, ws: web.WebSocketResponse):
        # self.logger.debug(f"{self}: reigstered client: {msg['data']['client_id']}")
        # Storing the client information
        self.ws_clients[msg["data"]["client_id"]] = ws

    async def _file_receive(self, request):

        reader = await request.multipart()

        # /!\ Don't forget to validate your inputs /!\

        # reader.next() will `yield` the fields of your form
        # Get the "meta" field
        field = await reader.next()
        assert field.name == "meta"
        meta_bytes = await field.read(decode=True)
        meta = json.loads(meta_bytes)

        # Get the "file" field
        field = await reader.next()
        assert field.name == "file"
        filename = field.filename

        # Attaching a UUID to prevent possible collision
        id = uuid.uuid4()
        filename_list = filename.split(".")
        uuid_filename = str(id) + "." + filename_list[1]

        # Create dst filepath
        dst_filepath = self.tempfolder / uuid_filename

        # Create the record and mark that is not complete
        # Keep record of the files sent!
        self.file_transfer_records[meta["sender_id"]][filename] = {
            "uuid": id,
            "uuid_filename": uuid_filename,
            "filename": filename,
            "dst_filepath": dst_filepath,
            "read": 0,
            "size": meta["size"],
            "complete": False,
        }

        # You cannot rely on Content-Length if transfer is chunked.
        read = 0
        total_size = meta["size"]
        prev_n = 0

        # Reading the buffer and writing the file
        async with aiofiles.open(dst_filepath, "wb") as f:
            with tqdm(
                total=1,
                unit="B",
                unit_scale=True,
                desc=f"File {field.filename}",
                miniters=1,
            ) as pbar:
                while True:
                    chunk = await field.read_chunk()  # 8192 bytes by default.
                    if not chunk:
                        break
                    await f.write(chunk)
                    read += len(chunk)
                    pbar.update(len(chunk) / total_size)
                    if (pbar.n - prev_n) > 0.05:
                        prev_n = pbar.n

        # After finishing, mark the size and that is complete
        self.file_transfer_records[meta["sender_id"]][filename].update(
            {"size": total_size, "complete": True}
        )

        return web.Response(
            text=f"{filename} sized of {total_size} successfully stored"
        )

    ####################################################################
    # IO Main Methods
    ####################################################################

    async def _write_ws(self, client_id: str, msg: Dict) -> bool:
        success = False

        if client_id in self.ws_clients:
            ws = self.ws_clients[client_id]
            success = await self._send_msg(ws, client_id, **msg)

        return success

    async def _websocket_handler(self, request):

        # self.logger.debug("Obtain WS connection")

        # Register new client
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        try:
            async for aiohttp_msg in ws:

                # Tracking the number of messages processed
                self.msg_processed_counter += 1

                # Extract the binary data and decoded it
                msg = aiohttp_msg.json()

                # Select the handler
                handler = self.ws_handlers[msg["signal"]]
                await handler(msg, ws)

                # self.logger.debug(f"{self}: after handler")

                # Send OK if requested
                if msg["ok"]:
                    try:
                        # self.logger.debug(f"{self}: sending OK")
                        await ws.send_json(
                            create_payload(GENERAL_MESSAGE.OK, {"uuid": msg["uuid"]})
                        )
                    except ConnectionResetError:
                        self.logger.warning(
                            f"{self}: ConnectionResetError, shutting down ws"
                        )
                        await ws.close()

        except Exception:
            self.logger.warning(traceback.format_exc())
            # self.logger.warning(f"{self}: WebSocket connection error: {e}")
        finally:

            # Close websocket
            await ws.close()

            # Remove client id
            target_client_id: Optional[str] = None
            for client_id, client_ws in self.ws_clients.values():
                if client_ws == ws:
                    target_client_id = client_id

            # If found, remove
            if target_client_id:
                client_ws = self.ws_clients[target_client_id]
                del self.ws_clients[target_client_id]

    ####################################################################
    # Server Utilities
    ####################################################################

    async def _send_msg(
        self,
        ws: web.WebSocketResponse,
        client_id: str,
        signal: enum.Enum,
        data: Dict,
        msg_uuid: str = str(uuid.uuid4()),
        ok: bool = False,
    ) -> bool:

        # First, check if the ws is still open
        if ws.closed:
            del self.ws_clients[client_id]
            return True

        # Create payload
        payload = create_payload(signal=signal, data=data, msg_uuid=msg_uuid, ok=ok)

        # Send the message
        try:
            await ws.send_json(payload)
        except ConnectionResetError:
            self.logger.warning(f"{self}: ConnectionResetError, shutting down ws")
            await ws.close()
            del self.ws_clients[client_id]
            return False

        # If ok, wait until ok
        if ok:
            await async_waiting_for(
                lambda: msg_uuid in self.uuid_records,
                timeout=config.get("comms.timeout.ok"),
            )

        return True

    ####################################################################
    # Server ASync Lifecycle API
    ####################################################################

    async def async_serve(self) -> bool:

        # Create record of message uuids
        self.uuid_records: collections.deque[str] = collections.deque(maxlen=100)

        # Use an application runner to run the web server
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "0.0.0.0", self.port)
        await self._site.start()

        # If port selected 0, then obtain the randomly selected port
        if self.port == 0:
            self.port = self._site._server.sockets[0].getsockname()[1]

        # Set flag
        self.running = True

        return True

    async def async_send(
        self, client_id: str, signal: enum.Enum, data: Dict, ok: bool = False
    ) -> bool:

        # Create uuid
        msg_uuid = str(uuid.uuid4())

        # Create msg container and execute writing coroutine
        msg = {"signal": signal, "data": data, "msg_uuid": msg_uuid, "ok": ok}
        success = await self._write_ws(client_id, msg)
        return success

    async def async_broadcast(
        self, signal: enum.Enum, data: Dict, ok: bool = False
    ) -> bool:

        # Create msg container and execute writing coroutine for all
        # clients
        msg = {"signal": signal, "data": data, "ok": ok}
        coros = []
        for client_id in self.ws_clients:
            coros.append(self._write_ws(client_id, msg))

        # Wait until all complete
        try:
            results = await asyncio.gather(*coros)
        except Exception:
            self.logger.error(traceback.format_exc())
            return False

        return all(results)

    async def async_shutdown(self) -> bool:

        # Only shutdown for the first time
        if not self.running:
            self.logger.debug(f"{self}: Tried to shutdown while not running.")
            return True

        for ws in self.ws_clients.values():
            try:
                await asyncio.wait_for(
                    ws.close(),
                    timeout=2,
                )
            except (asyncio.exceptions.TimeoutError, RuntimeError):
                pass

        # Cleanup and signal complete
        await asyncio.wait_for(self._runner.shutdown(), timeout=10)
        await asyncio.wait_for(self._runner.cleanup(), timeout=10)

        return True

    ####################################################################
    # Server Sync Lifecycle API
    ####################################################################

    def serve(self, blocking: bool = True) -> Optional[Future]:

        # Cannot serve twice
        if self.running:
            self.logger.warning(f"{self}: Requested to re-serve HTTP Server")
            return None

        future = self._exec_coro(self.async_serve())

        if blocking:
            future.result(timeout=config.get("comms.timeout.server-ready"))

        return future

    def send(
        self, client_id: str, signal: enum.Enum, data: Dict, ok: bool = False
    ) -> Future[bool]:
        return self._exec_coro(self.async_send(client_id, signal, data, ok))

    def broadcast(
        self, signal: enum.Enum, data: Dict, ok: bool = False
    ) -> Future[bool]:
        # Create msg container and execute writing coroutine for all
        # clients
        return self._exec_coro(self.async_broadcast(signal, data, ok))

    def move_transfer_files(self, dst: pathlib.Path, unzip: bool) -> bool:

        for name, filepath_dict in self.file_transfer_records.items():
            # Create a folder for the name
            named_dst = dst / name
            os.mkdir(named_dst)

            # Move all the content inside
            for filename, file_meta in filepath_dict.items():

                # Extract data
                filepath = file_meta["dst_filepath"]

                # Wait until filepath is completely written
                success = waiting_for(
                    condition=lambda: filepath.exists(),
                    timeout=config.get("comms.timeout.zip-time-write"),
                )

                if not success:
                    return False

                # If not unzip, just move it
                if not unzip:
                    shutil.move(filepath, named_dst / filename)

                # Otherwise, unzip, move content to the original folder,
                # and delete the zip file
                else:
                    shutil.unpack_archive(filepath, named_dst)

                    # Handling if temp folder includes a _ in the beginning
                    new_filename = file_meta["filename"]
                    if new_filename[0] == "_":
                        new_filename = new_filename[1:]
                    new_filename = new_filename.split(".")[0]

                    new_file = named_dst / new_filename

                    # Wait until file is ready
                    miss_counter = 0
                    delay = 0.5
                    timeout = config.get("comms.timeout.zip-time")

                    while not new_file.exists():
                        time.sleep(delay)
                        miss_counter += 1
                        if timeout < delay * miss_counter:
                            self.logger.error(
                                f"File zip unpacking took too long! - \
                                {name}:{filepath}:{new_file}"
                            )
                            return False

                    for file in new_file.iterdir():
                        shutil.move(file, named_dst)
                    shutil.rmtree(new_file)

        return True

    def shutdown(self, blocking: bool = True) -> Optional[Future]:

        future = self._exec_coro(self.async_shutdown())

        if blocking:
            future.result(timeout=config.get("comms.timeout.server-ready"))

        return future
