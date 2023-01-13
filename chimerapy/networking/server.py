# Built-in
from typing import Callable, Dict, Optional
import asyncio
import threading
import weakref

# Third-party
from aiohttp import web

# Internal Imports
from .async_loop_thread import AsyncLoopThread
from .utils import decode_payload, create_payload
from .enums import CLIENT_MESSAGE

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
        ws_handlers: Optional[Dict[str, Callable]] = None,
    ):

        # Store parameters
        self.name = name
        self.host = host
        self.port = port
        self.routes = routes
        self.ws_handlers = ws_handlers

        # Create AIOHTTP server
        self._app = web.Application()
        if self.routes:
            self._app.add_routes(self.routes)

        # WS support
        if self.ws_handlers:

            # Adding route for ws and other configuration
            self._app.add_routes([web.get("/ws", self._websocket_handler)])
            self.ws_clients = {}

            # Adding other essential ws handlers
            self.ws_handlers.update({CLIENT_MESSAGE.REGISTER: self._register_ws_client})

    def __str__(self):
        return f"<Server {self.name}>"

    async def _register_ws_client(self, ws: web.WebSocketResponse, msg):
        self.ws_clients[msg["data"]["client_name"]] = {"ws": ws}

    async def _read_ws(self, ws):
        logger.debug(f"{self}: reading")
        async for msg in ws:

            # Extract the binary data and decoded it
            msg = decode_payload(msg.data)
            logger.debug(f"{self}: read ws - {msg}")

            # Select the handler
            handler = self.ws_handlers[msg["signal"]]
            await handler(ws, msg)

    async def _write_ws(self, ws):
        logger.debug(f"{self}: writing")
        # while True:
        # msg = await self._send_msg_queue.get()
        # await asyncio.sleep(1)
        # ws.send_bytes(create_payload(**msg))

    async def _websocket_handler(self, request):
        logger.debug(f"{self}: received websocket response")

        # Register new client
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        logger.debug(f"{self}: received websocket response")

        # Establish read and write
        read_task = asyncio.create_task(self._read_ws(ws))
        write_task = asyncio.create_task(self._write_ws(ws))

        # Continue executing them
        await asyncio.gather(read_task, write_task)

        # Close the socket
        await ws.close()

        return ws

    async def _main(self):

        # Use an application runner to run the web server
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, self.host, self.port)
        await self._site.start()

        # Create flag to mark that server is ready
        self._server_ready.set()

    async def _web_shutdown(self):

        # Wait until runner has clean up
        await self._runner.cleanup()
        self._server_close.set()

    def serve(self):

        # Create async loop in thread
        self._server_ready = threading.Event()
        self._server_ready.clear()
        self._thread = AsyncLoopThread()
        self._thread.start()

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

    def shutdown(self):

        self._server_close = threading.Event()
        self._server_close.clear()
        self._thread.exec(self._web_shutdown)

        # Wait until server is ready
        flag = self._server_close.wait(timeout=10)
        if flag == 0:
            logger.debug(f"{self}: failed to shutting down")
            raise TimeoutError(f"{self}: failed to shutdown!")

        # Stop the async thread
        self._thread.stop()
