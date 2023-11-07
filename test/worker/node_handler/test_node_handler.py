import asyncio
import time
from dataclasses import dataclass
from typing import Optional, Union

import pytest
from aiodistbus import DEventBus, EntryPoint, EventBus, make_evented
from dataclasses_json import DataClassJsonMixin

import chimerapy.engine as cpe
from chimerapy.engine import config
from chimerapy.engine.data_protocols import RegisteredMethodData
from chimerapy.engine.states import WorkerState
from chimerapy.engine.worker.node_handler_service import NodeHandlerService

from ...conftest import linux_run_only
from ...core.networking.test_client_server import server
from ...node.streams.data_nodes import ImageNode, TabularNode, VideoNode

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


# Constants
assert server
NAME_CLASS_MAP = {
    "vn": VideoNode,
    "img_n": ImageNode,
    "tn": TabularNode,
    # "an": AudioNode, # causes problems!
}


class NodeWithRegisteredMethods(cpe.Node):
    def __init__(
        self, name: str, init_value: int = 0, debug_port: Optional[int] = None
    ):
        super().__init__(name=name, debug_port=debug_port)
        self.init_value = init_value

    def setup(self):
        self.logger.debug(f"{self}: executing SETUP")
        self.value = self.init_value

    def step(self):
        time.sleep(0.5)
        self.value += 1
        return self.value

    def teardown(self):
        self.logger.debug(f"{self}: executing TEARDOWN")

    # Default style
    @cpe.register
    async def printout(self):
        self.logger.debug(f"{self}: logging out value: {self.value}")
        return self.value

    # Style: blocking
    @cpe.register.with_config(params={"value": "Union[int, float]"}, style="blocking")
    async def set_value(self, value: Union[int, float]):
        self.value = value
        return value

    # Style: Reset
    @cpe.register.with_config(style="reset")
    async def reset(self):
        self.init_value = 100
        return 100


@pytest.fixture
def node_with_reg_methods(logreceiver):
    return NodeWithRegisteredMethods(name="RegNode1", debug_port=logreceiver.port)


@dataclass
class ExampleEvent(DataClassJsonMixin):
    msg: str


async def func(event: ExampleEvent):
    assert isinstance(event, ExampleEvent)
    logger.info(f"Received event {event}")


async def test_node_worker_link():

    # Local buses
    wb = EventBus()
    wdbus = DEventBus()
    nb = EventBus()

    # Create entrypoint
    ne = EntryPoint()
    await ne.connect(nb)
    await ne.on(f"worker.req", func, ExampleEvent)
    we = EntryPoint()
    await we.connect(wb)
    await we.on(f"node.status", func, ExampleEvent)

    # Link
    await nb.link(wdbus.ip, wdbus.port, ["node.*"], ["worker.*"])
    await wb.link(wdbus.ip, wdbus.port, ["worker.*"], ["node.*"])

    # Send message
    cevent = await ne.emit(f"node.status", ExampleEvent(msg="Hello"))
    sevent = await we.emit(f"worker.req", ExampleEvent(msg="Hello"))

    # Flush
    await wdbus.flush()

    # Assert
    assert cevent and cevent.id in we._received
    assert sevent and sevent.id in ne._received


async def node_status_func(data):
    logger.info(f"Received event {data}")


@pytest.fixture
async def node_handler(bus, entrypoint, dbus):

    # Requirements
    state = WorkerState(ip=dbus.ip, port=dbus.port)
    state = make_evented(state, bus=bus)
    logger = cpe._logger.getLogger("chimerapy-engine-worker")
    log_receiver = cpe._logger.get_node_id_zmq_listener()
    log_receiver.start(register_exit_handlers=True)

    # Create service
    node_handler = NodeHandlerService(
        name="node_handler",
        state=state,
        logger=logger,
        logreceiver=log_receiver,
    )
    await node_handler.attach(bus)
    await bus.link(dbus.ip, dbus.port, ["worker.*"], ["node.*"])
    # await entrypoint.on('node.*', node_status_func)

    # Necessary dependency
    # http_server = HttpServerService(name="http_server", state=state, logger=logger)
    # await http_server.attach(bus)

    await entrypoint.emit("start")
    yield node_handler
    await entrypoint.emit("shutdown")


async def test_create_service_instance(node_handler):
    ...


# @pytest.mark.parametrize("context", ["multiprocessing"])
# @pytest.mark.parametrize("context", ["threading"])
@pytest.mark.parametrize("context", ["multiprocessing", "threading"])
async def test_create_node(gen_node, node_handler, context):
    await node_handler.async_create_node(cpe.NodeConfig(gen_node, context=context))
    await node_handler.async_destroy_node(gen_node.id)


# @pytest.mark.skip(reason="Flaky")
@pytest.mark.parametrize(
    "context_order",
    [["multiprocessing", "threading"], ["threading", "multiprocessing"]],
)
async def test_create_node_along_with_different_context(
    node_handler, gen_node, con_node, context_order
):
    await node_handler.async_create_node(
        cpe.NodeConfig(gen_node, context=context_order[0])
    )
    await node_handler.async_create_node(
        cpe.NodeConfig(con_node, context=context_order[1])
    )
    await node_handler.async_destroy_node(gen_node.id)
    await node_handler.async_destroy_node(con_node.id)


@linux_run_only
async def test_create_unknown_node(node_handler):
    class UnknownNode(cpe.Node):
        def step(self):
            return 2

    node = UnknownNode(name="Unk1")
    node_id = node.id

    # Simple single node without connection
    node_config = cpe.NodeConfig(node)
    del UnknownNode

    await node_handler.async_create_node(node_config)
    await node_handler.async_destroy_node(node_id)


# @pytest.mark.parametrize("context", ["multiprocessing", "threading"])
@pytest.mark.parametrize("context", ["multiprocessing"])
# @pytest.mark.parametrize("context", ["threading"])
async def test_processing_node_pub_table(node_handler, gen_node, con_node, context):

    # Create
    await node_handler.async_create_node(cpe.NodeConfig(gen_node, context=context))
    await node_handler.async_create_node(
        cpe.NodeConfig(
            con_node,
            in_bound=[gen_node.id],
            in_bound_by_name=[gen_node.name],
            context=context,
        )
    )

    # Serve node pub table
    node_pub_table = node_handler._create_node_pub_table()
    await node_handler.async_process_node_pub_table(node_pub_table)

    # Destroy
    await node_handler.async_destroy_node(gen_node.id)
    await node_handler.async_destroy_node(con_node.id)


@pytest.mark.parametrize("context", ["multiprocessing"])
# @pytest.mark.parametrize("context", ["multiprocessing", "threading"])
async def test_starting_node(node_handler, gen_node, context):

    await node_handler.async_create_node(cpe.NodeConfig(gen_node, context=context))
    await node_handler.async_start_nodes()
    await asyncio.sleep(1)
    await node_handler.async_stop_nodes()
    await node_handler.async_destroy_node(gen_node.id)


@pytest.mark.parametrize("context", ["multiprocessing"])
# @pytest.mark.parametrize("context", ["multiprocessing", "threading"])
async def test_record_and_collect(node_handler, context):

    nodes = []
    for node_name, node_class in NAME_CLASS_MAP.items():
        nodes.append(node_class(name=node_name))

    # Simple single node without connection
    for node in nodes:
        await node_handler.async_create_node(cpe.NodeConfig(node, context=context))

    await node_handler.async_start_nodes()
    await asyncio.sleep(1)

    await node_handler.async_record_nodes()
    await asyncio.sleep(1)

    await node_handler.async_stop_nodes()
    await node_handler.async_collect()

    for node in nodes:
        await node_handler.async_destroy_node(node.id)

    for node_name in NAME_CLASS_MAP:
        assert (node_handler.state.tempfolder / node_name).exists()


async def test_registered_method_with_concurrent_style(
    node_handler, node_with_reg_methods
):

    # Create the node
    await node_handler.async_create_node(cpe.NodeConfig(node_with_reg_methods))

    # Execute the registered method (with config)
    # logger.debug(f"Requesting registered method")
    reg_method_data = RegisteredMethodData(
        node_id=node_with_reg_methods.id,
        method_name="printout",
    )
    # logger.debug(f"Requesting registered method: {reg_method_data}")
    results = await node_handler.async_request_registered_method(reg_method_data)
    # logger.debug(f"Results: {results}")

    await node_handler.async_destroy_node(node_with_reg_methods.id)
    assert (
        results["success"]
        and isinstance(results["output"], int)
        and results["output"] >= 0
    )


async def test_registered_method_with_params_and_blocking_style(
    node_handler, node_with_reg_methods
):

    # Create the node
    await node_handler.async_create_node(cpe.NodeConfig(node_with_reg_methods))

    # Execute the registered method (with config)
    reg_method_data = RegisteredMethodData(
        node_id=node_with_reg_methods.id,
        method_name="set_value",
        params={"value": -100},
    )
    results = await node_handler.async_request_registered_method(reg_method_data)

    await node_handler.async_destroy_node(node_with_reg_methods.id)
    assert (
        results["success"]
        and isinstance(results["output"], int)
        and results["output"] < -50
    )


async def test_registered_method_with_reset_style(node_handler, node_with_reg_methods):

    # Create the node
    await node_handler.async_create_node(cpe.NodeConfig(node_with_reg_methods))

    # Execute the registered method (with config)
    reg_method_data = RegisteredMethodData(
        node_id=node_with_reg_methods.id,
        method_name="reset",
    )
    results = await node_handler.async_request_registered_method(reg_method_data)

    await node_handler.async_destroy_node(node_with_reg_methods.id)

    assert (
        results["success"]
        and isinstance(results["output"], int)
        and results["output"] >= 100
    )


@pytest.mark.parametrize("context", ["multiprocessing"])
# @pytest.mark.parametrize("context", ["multiprocessing", "threading"])
async def test_gather(node_handler, gen_node, context):

    await node_handler.async_create_node(cpe.NodeConfig(gen_node, context=context))
    await node_handler.async_start_nodes()
    await asyncio.sleep(1)
    await node_handler.async_stop_nodes()

    results = await node_handler.async_gather()
    assert len(results) > 0

    await node_handler.async_destroy_node(gen_node.id)


@pytest.mark.parametrize("context", ["multiprocessing"])
# @pytest.mark.parametrize("context", ["multiprocessing", "threading"])
# @pytest.mark.parametrize("context", ["threading"])
async def test_diagnostics(node_handler, gen_node, context):

    config.set("diagnostics.interval", 0.5)
    config.set("diagnostics.logging-enabled", True)

    await node_handler.async_create_node(cpe.NodeConfig(gen_node, context=context))
    await node_handler.async_start_nodes()
    await node_handler.async_diagnostics(True)
    await asyncio.sleep(2)
    await node_handler.async_stop_nodes()

    await node_handler.async_destroy_node(gen_node.id)

    path = node_handler.state.tempfolder / gen_node.name / "diagnostics.csv"
    assert path.exists()
