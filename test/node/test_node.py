import time
import os
import pathlib
import asyncio
from typing import Type

import pytest
import multiprocessing as mp

import chimerapy.engine as cpe
from chimerapy.engine.node.worker_comms_service import WorkerCommsService
from chimerapy.engine.node.node_config import NodeConfig
from chimerapy.engine.networking.async_loop_thread import AsyncLoopThread
from chimerapy.engine.networking.enums import WORKER_MESSAGE
from chimerapy.engine.eventbus import EventBus, Event

from .test_worker_comms import server

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = CWD / "data"

# Added to prevent Ruff dropping "unused" import AKA fixtures
__all__ = ["server"]


class StepNode(cpe.Node):
    def step(self):
        time.sleep(0.1)
        self.logger.debug(f"{self}: step")
        return 1


class AsyncStepNode(cpe.Node):
    async def step(self):
        await asyncio.sleep(0.1)
        self.logger.debug(f"{self}: step")
        return 1


class MainNode(cpe.Node):
    def main(self):
        while self.running:
            time.sleep(0.1)
            self.logger.debug(f"{self}: step")
        return 1


class AsyncMainNode(cpe.Node):
    async def main(self):
        while self.running:
            await asyncio.sleep(0.1)
            self.logger.debug(f"{self}: step")
        return 1


@pytest.fixture
def eventbus():

    # Event Loop
    thread = AsyncLoopThread()
    thread.start()
    eventbus = EventBus(thread=thread)

    return eventbus


@pytest.fixture
def worker_comms_setup(server):

    # Create the service
    worker_comms = WorkerCommsService(
        "worker_comms",
        host=server.host,
        port=server.port,
        node_config=NodeConfig(),
    )

    return (worker_comms, server)


@pytest.mark.parametrize("node_cls", [StepNode, AsyncStepNode, MainNode, AsyncMainNode])
def test_running_node_in_same_process(logreceiver, node_cls: Type[cpe.Node], eventbus):

    # Create the node
    node = node_cls(name="step", debug_port=logreceiver.port)

    # Running
    logger.debug(f"Running Node: {node_cls}")
    node.run(blocking=False, eventbus=eventbus)

    # Wait
    time.sleep(1)

    logger.debug("Shutting down Node")
    node.shutdown()


@pytest.mark.parametrize(
    "node_cls",
    [
        StepNode,
        AsyncStepNode,
        MainNode,
        AsyncMainNode,
    ],
)
def test_lifecycle_start_record_stop(logreceiver, node_cls: Type[cpe.Node], eventbus):

    # Create the node
    node = node_cls(name="step", debug_port=logreceiver.port)

    # Running
    logger.debug(f"Running Node: {node_cls}")
    node.run(blocking=False, eventbus=eventbus)

    # Wait
    time.sleep(0.5)
    eventbus.send(Event("start")).result()
    logger.debug("Finish start")
    time.sleep(0.5)
    eventbus.send(Event("record")).result()
    logger.debug("Finish record")
    time.sleep(0.5)
    eventbus.send(Event("stop")).result()
    logger.debug("Finish stop")
    time.sleep(0.5)
    eventbus.send(Event("collect")).result()
    logger.debug("Finish collect")

    logger.debug("Shutting down Node")
    node.shutdown()


@pytest.mark.parametrize(
    "node_cls",
    [
        StepNode,
        AsyncStepNode,
        MainNode,
        AsyncMainNode,
    ],
)
def test_node_in_process(logreceiver, node_cls: Type[cpe.Node], worker_comms_setup):
    worker_comms, server = worker_comms_setup

    # Create the node
    node = node_cls(name="step", debug_port=logreceiver.port)
    id = node.id

    # Add worker_comms
    node.add_worker_comms(worker_comms)
    node.running = mp.Value("i", True)

    # Adding shared variable that would be typically added by the Worker
    p = mp.Process(target=node.run)
    p.start()
    time.sleep(0.5)

    # Run method
    server.send(client_id=id, signal=WORKER_MESSAGE.START_NODES, data={}, ok=True)
    time.sleep(0.5)

    server.send(client_id=id, signal=WORKER_MESSAGE.RECORD_NODES, data={}, ok=True)
    time.sleep(0.5)

    server.send(client_id=id, signal=WORKER_MESSAGE.STOP_NODES, data={}, ok=True)
    time.sleep(0.5)

    server.send(client_id=id, signal=WORKER_MESSAGE.REQUEST_COLLECT, data={}, ok=True)
    time.sleep(1)

    node.shutdown()
    logger.debug("Shutting down Node")

    p.join()
