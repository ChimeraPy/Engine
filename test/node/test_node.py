import asyncio
import multiprocessing as mp
import os
import pathlib
import time
from threading import Thread
from typing import Type

import pytest

import chimerapy.engine as cpe
from chimerapy.engine import config
from chimerapy.engine.data_protocols import WorkerInfo
from chimerapy.engine.node.node_config import NodeConfig
from chimerapy.engine.node.worker_comms_service import WorkerCommsService

from ..conftest import linux_run_only

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"


class StepNode(cpe.Node):
    def setup(self):
        self.logger.debug(f"{self}: setup")

    def step(self):
        time.sleep(0.1)
        self.logger.debug(f"{self}: step")
        return 1

    def teardown(self):
        self.logger.debug(f"{self}: teardown")


class AsyncStepNode(cpe.Node):
    async def setup(self):
        self.logger.debug(f"{self}: setup")

    async def step(self):
        await asyncio.sleep(0.1)
        self.logger.debug(f"{self}: step")
        return 1

    async def teardown(self):
        self.logger.debug(f"{self}: teardown")


class MainNode(cpe.Node):
    def setup(self):
        self.logger.debug(f"{self}: setup")

    def main(self):
        while self.running:
            time.sleep(0.1)
            self.logger.debug(f"{self}: step")
        return 1

    def teardown(self):
        self.logger.debug(f"{self}: teardown")


class AsyncMainNode(cpe.Node):
    async def setup(self):
        self.logger.debug(f"{self}: setup")

    async def main(self):
        while self.running:
            await asyncio.sleep(0.1)
            self.logger.debug(f"{self}: step")
        return 1

    async def teardown(self):
        self.logger.debug(f"{self}: teardown")


@pytest.mark.parametrize("node_cls", [StepNode, AsyncStepNode, MainNode, AsyncMainNode])
async def test_running_node_async_in_same_process(
    logreceiver, node_cls: Type[cpe.Node], bus
):
    node = node_cls(name="step", debug_port=logreceiver.port)
    task = asyncio.create_task(node.arun(bus=bus))
    await node.ashutdown()
    await task


@pytest.mark.parametrize("node_cls", [StepNode, AsyncStepNode, MainNode, AsyncMainNode])
def test_running_node_in_same_process(logreceiver, node_cls: Type[cpe.Node], bus):
    node = node_cls(name="step", debug_port=logreceiver.port)
    thread = Thread(target=node.run, args=(bus,))
    thread.start()
    node.shutdown()
    thread.join()


@pytest.mark.parametrize("node_cls", [StepNode, AsyncStepNode, MainNode, AsyncMainNode])
def test_running_node_in_process(logreceiver, node_cls: Type[cpe.Node]):
    node = node_cls(name="step", debug_port=logreceiver.port)

    running = mp.Value("i", True)
    p = mp.Process(
        target=node.run,
        args=(
            None,
            running,
        ),
    )
    p.start()
    node.shutdown()
    running.value = False
    p.join()


@pytest.mark.parametrize(
    "node_cls",
    [
        StepNode,
        AsyncStepNode,
        MainNode,
        AsyncMainNode,
    ],
)
async def test_lifecycle_start_record_stop(
    logreceiver, node_cls: Type[cpe.Node], bus, entrypoint
):

    # Create the node
    node = node_cls(name="step", debug_port=logreceiver.port)

    # Running
    logger.debug(f"Running Node: {node_cls}")
    task = asyncio.create_task(node.arun(bus=bus))
    logger.debug(f"Outside Node: {node_cls}")
    await asyncio.sleep(1)  # Necessary to let the Node's services to startup

    # Wait
    await entrypoint.emit("start")
    logger.debug("Finish start")
    await asyncio.sleep(0.5)

    await entrypoint.emit("record")
    logger.debug("Finish record")
    await asyncio.sleep(0.5)

    await entrypoint.emit("stop")
    logger.debug("Finish stop")
    await asyncio.sleep(0.5)

    await entrypoint.emit("collect")
    logger.debug("Finish collect")

    logger.debug("Shutting down Node")
    await node.ashutdown()
    await task


@pytest.mark.parametrize(
    "node_cls",
    [
        StepNode,
        # AsyncStepNode,
        # MainNode,
        # AsyncMainNode,
    ],
)
async def test_node_in_process(
    logreceiver, node_cls: Type[cpe.Node], dbus, dentrypoint
):

    # Create the node
    node = node_cls(name="step", debug_port=logreceiver.port)

    # Pass mock worker information
    worker_info = WorkerInfo(
        host=dbus.ip,
        port=dbus.port,
        logdir=TEST_DATA_DIR,
        node_config=NodeConfig(),
        config=config.config,
    )
    node.set_worker_info(worker_info)
    running = mp.Value("i", True)

    # Adding shared variable that would be typically added by the Worker
    p = mp.Process(
        target=node.run,
        args=(
            None,
            running,
        ),
    )
    p.start()
    logger.debug(f"Running Node: {node_cls}")
    await asyncio.sleep(1)

    # Run method
    await dentrypoint.emit("start")
    logger.debug("Finish start")
    await asyncio.sleep(0.25)

    await dentrypoint.emit("record")
    logger.debug("Finish record")
    await asyncio.sleep(0.25)

    await dentrypoint.emit("stop")
    logger.debug("Finish stop")
    await asyncio.sleep(0.25)

    await dentrypoint.emit("collect")
    logger.debug("Finish collect")
    await asyncio.sleep(0.25)

    node.shutdown()
    running.value = False
    logger.debug("Shutting down Node")

    p.join()


@linux_run_only
@pytest.mark.parametrize("context", ["fork", "spawn"])
async def test_node_in_process_different_context(
    logreceiver, dbus, dentrypoint, context
):

    # Create the node
    node = StepNode(name="step", debug_port=logreceiver.port)

    # Add worker_info
    worker_info = WorkerInfo(
        host=dbus.ip,
        port=dbus.port,
        logdir=TEST_DATA_DIR,
        node_config=NodeConfig(),
        config=config.config,
    )
    node.set_worker_info(worker_info)

    # Adding shared variable that would be typically added by the Worker
    running = mp.Value("i", True)
    ctx = mp.get_context(context)
    p = ctx.Process(
        target=node.run,
        args=(
            None,
            running,
        ),
    )
    p.start()
    await asyncio.sleep(0.5)

    # Run method
    await dentrypoint.emit("start")
    logger.debug("Finish start")
    await asyncio.sleep(0.25)

    await dentrypoint.emit("record")
    logger.debug("Finish record")
    await asyncio.sleep(0.25)

    await dentrypoint.emit("stop")
    logger.debug("Finish stop")
    await asyncio.sleep(0.25)

    await dentrypoint.emit("collect")
    logger.debug("Finish collect")
    await asyncio.sleep(0.25)

    node.shutdown()
    running.value = False
    logger.debug("Shutting down Node")

    p.join()
