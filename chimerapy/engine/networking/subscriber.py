# Built-in
import asyncio
import uuid
from dataclasses import dataclass
from typing import Callable, Coroutine, Dict, Optional, Union

# Third-party Imports
import zmq
import zmq.asyncio

# Internal Imports
# Logging
from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy-engine-networking")

# Reference:
# https://pyzmq.readthedocs.io/en/latest/api/zmq.html?highlight=socket#polling


@dataclass
class Subscription:
    id: str
    host: str
    port: int
    topic: str
    socket: zmq.Socket


class Subscriber:
    def __init__(self, ctx: Optional[zmq.asyncio.Context] = None):

        # Parameters
        if ctx:
            self._zmq_context = ctx
        else:
            self._zmq_context = zmq.asyncio.Context()

        # State variables
        self._on_receive: Optional[Union[Callable, Coroutine]] = None
        self._running: bool = False
        self.subscriptions: Dict[str, Subscription] = {}
        self.socket_to_sub_name_mapping: Dict[zmq.Socket, str] = {}

    def subscribe(
        self, host: str, port: int, topic: str = "", id: Optional[str] = None
    ):

        if id is None:
            id = str(uuid.uuid4())

        # Create socket
        _zmq_socket = self._zmq_context.socket(zmq.SUB)
        _zmq_socket.setsockopt(zmq.CONFLATE, 1)
        _zmq_socket.connect(f"tcp://{host}:{port}")
        _zmq_socket.subscribe(topic.encode("utf-8"))

        # Create subscription
        sub = Subscription(
            id=id,
            host=host,
            port=port,
            topic=topic,
            socket=_zmq_socket,
        )
        self.subscriptions[id] = sub
        self.socket_to_sub_name_mapping[_zmq_socket] = id

    def unsubscribe(self, id: str):

        # Close the socket
        sub = self.subscriptions[id]
        sub.socket.close()

        # Remove from the list
        del self.subscriptions[id]
        del self.socket_to_sub_name_mapping[sub.socket]

    def __str__(self):
        return f"<Subscriber for {list(self.subscriptions.keys())}>"

    @property
    def running(self):
        return self._running

    async def poll_inputs(self):

        while self._running:

            # Wait until we get data from any of the subscribers
            event_list = await self.poller.poll(timeout=10)
            events = dict(event_list)

            # Empty if no events
            if len(events) == 0:
                continue

            datas: Dict[str, bytes] = {}
            for s in events:
                data = await s.recv()
                datas[self.socket_to_sub_name_mapping[s]] = data

            if self._on_receive:
                if asyncio.iscoroutinefunction(self._on_receive):
                    await self._on_receive(datas)
                else:
                    self._on_receive(datas)

    async def start(self):

        # Warning if no "on" function is specified
        if not self._on_receive:
            logger.warning(f"{self}: no 'on' function specified")

        # Create poller to make smart non-blocking IO
        self.poller = zmq.asyncio.Poller()
        for sub in self.subscriptions.values():
            self.poller.register(sub.socket, zmq.POLLIN)

        # Create a task to run the receive loop
        self._running = True
        self._poll_task = asyncio.create_task(self.poll_inputs())

    def on_receive(self, fn: Union[Callable, Coroutine]):
        self._on_receive = fn

    async def shutdown(self):

        if self._running:

            # Stop the thread
            self._running = False
            await self._poll_task

            subs = list(self.subscriptions.keys())
            for sub in subs:
                self.unsubscribe(sub)
