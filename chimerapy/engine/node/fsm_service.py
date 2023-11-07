import logging
from typing import Optional

from aiodistbus import registry

from ..service import Service
from ..states import NodeState


class FSMService(Service):
    def __init__(self, name: str, state: NodeState, logger: logging.Logger):
        super().__init__(name=name)

        # Save params
        self.state = state
        self.logger = logger

    @registry.on("initialize", namespace=f"{__name__}.FSMService")
    async def init(self):
        self.state.fsm = "INITIALIZED"

    @registry.on("setup", namespace=f"{__name__}.FSMService")
    async def setup(self):
        self.state.fsm = "READY"

    @registry.on("connected", namespace=f"{__name__}.FSMService")
    async def setup_connections(self):
        self.state.fsm = "CONNECTED"

    @registry.on("start", namespace=f"{__name__}.FSMService")
    async def start(self):
        self.state.fsm = "PREVIEWING"

    @registry.on("record", namespace=f"{__name__}.FSMService")
    async def record(self):
        self.state.fsm = "RECORDING"

    @registry.on("stop", namespace=f"{__name__}.FSMService")
    async def stop(self):
        self.state.fsm = "STOPPED"

    @registry.on("collect", namespace=f"{__name__}.FSMService")
    async def collect(self):
        self.state.fsm = "SAVED"

    @registry.on("teardown", namespace=f"{__name__}.FSMService")
    async def teardown(self):
        self.state.fsm = "SHUTDOWN"
