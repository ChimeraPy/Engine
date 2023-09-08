import logging
from typing import Dict

from ..states import NodeState
from ..service import Service
from ..eventbus import EventBus, TypedObserver, Event
from .events import NodeStateChangedEvent


class FSMService(Service):
    def __init__(
        self, name: str, state: NodeState, eventbus: EventBus, logger: logging.Logger
    ):
        super().__init__(name=name)

        # Save params
        self.state = state
        self.eventbus = eventbus
        self.logger = logger

        # Add observers
        self.observers: Dict[str, TypedObserver] = {
            "initialize": TypedObserver(
                "initialize", on_asend=self.init, handle_event="drop"
            ),
            "setup": TypedObserver("setup", on_asend=self.setup, handle_event="drop"),
            "start": TypedObserver("start", on_asend=self.start, handle_event="drop"),
            "setup_connections": TypedObserver(
                "setup_connections",
                on_asend=self.setup_connections,
                handle_event="drop",
            ),
            "record": TypedObserver(
                "record", on_asend=self.record, handle_event="drop"
            ),
            "stop": TypedObserver("stop", on_asend=self.stop, handle_event="drop"),
            "collect": TypedObserver(
                "collect", on_asend=self.collect, handle_event="drop"
            ),
            "teardown": TypedObserver(
                "teardown", on_asend=self.teardown, handle_event="drop"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    async def emit_change(self):
        data = NodeStateChangedEvent(state=self.state)
        await self.eventbus.asend(Event("NodeState.changed", data))

    async def init(self):
        self.state.fsm = "INITIALIZED"
        await self.emit_change()

    async def setup(self):
        self.state.fsm = "READY"
        await self.emit_change()

    async def setup_connections(self):
        self.state.fsm = "CONNECTED"
        await self.emit_change()

    async def start(self):
        self.state.fsm = "PREVIEWING"
        await self.emit_change()

    async def record(self):
        self.state.fsm = "RECORDING"
        await self.emit_change()

    async def stop(self):
        self.state.fsm = "STOPPED"
        await self.emit_change()

    async def collect(self):
        self.state.fsm = "SAVED"
        await self.emit_change()

    async def teardown(self):
        self.state.fsm = "SHUTDOWN"
        await self.emit_change()
