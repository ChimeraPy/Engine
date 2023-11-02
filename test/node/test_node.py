import asyncio
import multiprocessing as mp
import os
import pathlib
import time
from threading import Thread
from typing import Type

import pytest
from aiodistbus import EntryPoint, EventBus

import chimerapy.engine as cpe
from chimerapy.engine import config
from chimerapy.engine.data_protocols import NodePubEntry, NodePubTable
from chimerapy.engine.networking.enums import WORKER_MESSAGE
from chimerapy.engine.node.node_config import NodeConfig
from chimerapy.engine.node.worker_comms_service import WorkerCommsService
from chimerapy.engine.utils import get_ip_address

from ..conftest import ConsumeNode, GenNode, linux_run_only
from .test_worker_comms import mock_worker

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()

# Constants
CWD = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_DATA_DIR = CWD / "data"

# Added to prevent Ruff dropping "unused" import AKA fixtures
__all__ = ["mock_worker"]


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


@pytest.fixture
async def worker_comms2(mock_worker):

    config.set("diagnostics.logging-enabled", True)

    # Create the service
    worker_comms = WorkerCommsService(
        "worker_comms",
        host=mock_worker.server.host,
        port=mock_worker.server.port,
        node_config=NodeConfig(),
        worker_config=config.config,
    )

    return worker_comms


@pytest.mark.parametrize("node_cls", [StepNode, AsyncStepNode, MainNode, AsyncMainNode])
async def test_running_node_async_in_same_process(
    logreceiver, node_cls: Type[cpe.Node], bus
):
    node = node_cls(name="step", debug_port=logreceiver.port)
    task = asyncio.create_task(node.arun(bus=bus))
    await asyncio.sleep(0.1)
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
        AsyncStepNode,
        MainNode,
        AsyncMainNode,
    ],
)
async def test_node_in_process(
    logreceiver, node_cls: Type[cpe.Node], worker_comms2, mock_worker
):

    # Create the node
    node = node_cls(name="step", debug_port=logreceiver.port)
    id = node.id

    # Add worker_comms
    node.add_worker_comms(worker_comms2)
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
    await asyncio.sleep(0.5)

    # Run method
    await mock_worker.server.async_send(
        client_id=id, signal=WORKER_MESSAGE.START_NODES, data={}, ok=True
    )
    await asyncio.sleep(0.25)

    await mock_worker.server.async_send(
        client_id=id, signal=WORKER_MESSAGE.RECORD_NODES, data={}, ok=True
    )
    await asyncio.sleep(0.25)

    await mock_worker.server.async_send(
        client_id=id, signal=WORKER_MESSAGE.STOP_NODES, data={}, ok=True
    )
    await asyncio.sleep(0.25)

    await mock_worker.server.async_send(
        client_id=id, signal=WORKER_MESSAGE.REQUEST_COLLECT, data={}, ok=True
    )
    await asyncio.sleep(0.25)

    node.shutdown()
    running.value = False
    logger.debug("Shutting down Node")

    p.join()


@linux_run_only
@pytest.mark.parametrize("context", ["fork", "spawn"])
async def test_node_in_process_different_context(
    logreceiver, worker_comms2, context, mock_worker
):

    # Create the node
    node = StepNode(name="step", debug_port=logreceiver.port)
    id = node.id

    # Add worker_comms
    node.add_worker_comms(worker_comms2)

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
    await mock_worker.server.async_send(
        client_id=id, signal=WORKER_MESSAGE.START_NODES, data={}, ok=True
    )
    await asyncio.sleep(0.25)

    await mock_worker.server.async_send(
        client_id=id, signal=WORKER_MESSAGE.RECORD_NODES, data={}, ok=True
    )
    await asyncio.sleep(0.25)

    await mock_worker.server.async_send(
        client_id=id, signal=WORKER_MESSAGE.STOP_NODES, data={}, ok=True
    )
    await asyncio.sleep(0.25)

    await mock_worker.server.async_send(
        client_id=id, signal=WORKER_MESSAGE.REQUEST_COLLECT, data={}, ok=True
    )
    await asyncio.sleep(0.25)

    node.shutdown()
    running.value = False
    logger.debug("Shutting down Node")

    p.join()


async def test_node_connection(logreceiver, mock_worker):

    # Create evenbus for each node
    g_eventbus = EventBus()
    c_eventbus = EventBus()
    g_entrypoint = EntryPoint()
    c_entrypoint = EntryPoint()
    await g_entrypoint.connect(g_eventbus)
    await c_entrypoint.connect(c_eventbus)

    # Create the node
    gen_node = GenNode(name="Gen1", debug_port=logreceiver.port, id="Gen1")
    con_node = ConsumeNode(name="Con1", debug_port=logreceiver.port, id="Con1")

    # Create the service
    gen_worker_comms = WorkerCommsService(
        "worker_comms",
        host=mock_worker.server.host,
        port=mock_worker.server.port,
        node_config=NodeConfig(gen_node, out_bound=["Gen1"]),
    )

    # Create the service
    con_worker_comms = WorkerCommsService(
        "worker_comms",
        host=mock_worker.server.host,
        port=mock_worker.server.port,
        node_config=NodeConfig(
            con_node, in_bound=["Gen1"], in_bound_by_name=["Gen1"], follow="Gen1"
        ),
    )

    # Add worker comms
    gen_node.add_worker_comms(gen_worker_comms)
    con_node.add_worker_comms(con_worker_comms)

    # Running
    logger.debug(f"Running Nodes: {gen_node.state}, {con_node.state}")
    g_task = asyncio.create_task(gen_node.arun(bus=g_eventbus))
    c_task = asyncio.create_task(con_node.arun(bus=c_eventbus))
    logger.debug("Finish run")

    # Create the connections
    node_pub_table = NodePubTable()
    for id, node_state in mock_worker.node_states.items():
        ip = get_ip_address()  # necessary
        node_pub_table.table[id] = NodePubEntry(ip=ip, port=node_state.port)
    logger.debug("Before broadcast")
    await mock_worker.server.async_broadcast(
        signal=WORKER_MESSAGE.BROADCAST_NODE_SERVER, data=node_pub_table.to_dict()
    )
    logger.debug("Finish broadcast")

    # Wait
    await g_entrypoint.emit("start")
    await c_entrypoint.emit("start")
    logger.debug("Finish start")
    await asyncio.sleep(2)
    await g_entrypoint.emit("stop")
    await c_entrypoint.emit("stop")
    logger.debug("Finish stop")

    logger.debug("Shutting down Node")
    await gen_node.ashutdown()
    await con_node.ashutdown()
    await g_task
    await c_task
