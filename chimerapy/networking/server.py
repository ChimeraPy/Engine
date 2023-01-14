# Built-in
from typing import Callable, Dict, Optional, Any, Union
import asyncio
import threading
import uuid
import collections
import time

# Third-party
from aiohttp import web

# Internal Imports
from .async_loop_thread import AsyncLoopThread
from .utils import decode_payload, create_payload
from .enums import CLIENT_MESSAGE, GENERAL_MESSAGE

# Logging
from .. import _logger

logger = _logger.getLogger("chimerapy-networking")

# References
# https://gist.github.com/dmfigol/3e7d5b84a16d076df02baa9f53271058
# https://docs.aiohttp.org/en/stable/web_advanced.html#application-runners
# https://stackoverflow.com/questions/58455058/how-do-i-avoid-the-loop-argument
# https://docs.aiohttp.org/en/stable/web_advanced.html?highlight=weakref#graceful-shutdown


class Server:
    def __init__(
        self,
        name: str,
        port: int,
        host: Optional[str] = "localhost",
        routes: Optional[Dict[str, Callable]] = None,
        ws_handlers: Optional[Dict[int, Callable]] = None,
    ):

        # Store parameters
        self.name = name
        self.host = host
        self.port = port
        self.routes = routes
        self.ws_handlers = ws_handlers

        # Using flag for marking if system should be running
        self.running = threading.Event()
        self.running.clear()

        # Create AIOHTTP server
        self._app = web.Application()
        if self.routes:
            self._app.add_routes(self.routes)

        # WS support
        if self.ws_handlers:

            # Adding route for ws and other configuration
            self._app.add_routes([web.get("/ws", self._websocket_handler)])
            self.ws_clients: Dict[str, Dict[str, Any]] = {}

            # Adding other essential ws handlers
            self.ws_handlers.update(
                {
                    GENERAL_MESSAGE.OK: self._ok,
                    CLIENT_MESSAGE.REGISTER: self._register_ws_client,
                }
            )

    def __str__(self):
        return f"<Server {self.name}>"

    ####################################################################
    # Server WS Handlers
    ####################################################################

    async def _ok(self, msg: Dict, ws: web.WebSocketResponse):
        self.uuid_records.append(msg["data"]["uuid"])

    async def _register_ws_client(self, msg: Dict, ws: web.WebSocketResponse):

        # Create a queue to make send request to
        client_name = msg["data"]["client_name"]
        client_queue = asyncio.Queue()

        # Storing the client information
        self.ws_clients[client_name] = {"ws": ws, "send_queue": client_queue}

        # Only until the Client has been registered can we send messages
        # to it
        write_task = asyncio.create_task(self._write_ws(client_name, ws, client_queue))

    ####################################################################
    # IO Main Methods
    ####################################################################

    async def _read_ws(self, ws: web.WebSocketResponse):
        logger.debug(f"{self}: reading")
        async for aiohttp_msg in ws:

            # Extract the binary data and decoded it
            msg = decode_payload(aiohttp_msg.data)
            logger.debug(f"{self}: read - {msg}")

            # Select the handler
            handler = self.ws_handlers[msg["signal"]]
            await handler(msg, ws)

            # Send OK if requested
            if msg["ok"]:
                logger.debug(f"{self}: sending OK for {msg['uuid']}")
                await ws.send_bytes(
                    create_payload(GENERAL_MESSAGE.OK, {"uuid": msg["uuid"]})
                )

    async def _write_ws(
        self, client_name: str, ws: web.WebSocketResponse, queue: asyncio.Queue
    ):
        logger.debug(f"{self}: writing for <Client {client_name}>")

        while self.running.is_set():
            msg = await queue.get()
            logger.debug(f"{self}: write - {msg}")
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
    # Client Utilities
    ####################################################################

    async def _send_msg(
        self, ws: web.WebSocketResponse, signal: int, data: Dict, ok: bool = False
    ):

        # Create uuid
        msg_uuid = str(uuid.uuid4())

        # Create payload
        payload = create_payload(signal=signal, data=data, msg_uuid=msg_uuid, ok=ok)

        # Send the message
        await ws.send_bytes(payload)
        logger.debug(f"{self}: _send_msg -> {signal}")

        # If ok, wait until ok
        if ok:
            for i in range(10):
                if msg_uuid in self.uuid_records:
                    return
                else:
                    await asyncio.sleep(1)

            logger.error(f"{self}: OK was not received!")

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

        # Create flag to mark that server is ready
        self._server_ready.set()

    async def _server_shutdown(self):

        # Cleanup and signal complete
        await self._runner.cleanup()
        self._server_shutdown_complete.set()

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
        flag = self._server_ready.wait(timeout=10)
        if flag == 0:
            logger.debug(f"{self}: failed to start, shutting down!")
            self.shutdown()
            raise TimeoutError(f"{self}: failed to start, shutting down!")
        else:
            logger.debug(f"{self}: running at {self.host}:{self.port}")

    def send(
        self,
        client_name: str,
        signal: int,
        data: Dict,
        ok: bool = False,
    ):

        # Create msg container
        msg = {"signal": signal, "data": data, "ok": ok}

        # Then make the request via the queue
        client_queue = self.ws_clients[client_name]["send_queue"]
        self._thread.exec_noncoro(client_queue.put_nowait, args=[msg])

    def flush(self, timeout: Optional[Union[float, int]] = None):

        counter = 0
        for client_name in self.ws_clients:
            queue = self.ws_clients[client_name]["send_queue"]
            while True:
                if queue.qsize() == 0:
                    break
                else:
                    time.sleep(0.1)
                    counter += 1
                    if timeout and (counter * 0.1) > timeout:
                        raise TimeoutError("Flush took too long!")

    def shutdown(self):

        # Use client event for shutdown
        self._server_shutdown_complete = threading.Event()
        self._server_shutdown_complete.clear()

        # Execute shutdown
        self._thread.exec(self._server_shutdown)

        # Wait for it
        if not self._server_shutdown_complete.wait(timeout=5):
            logger.warning(f"{self}: failed to gracefully shutdown")

        # Stop the async thread
        self._thread.stop()
