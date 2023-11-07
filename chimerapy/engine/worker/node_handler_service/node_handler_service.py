import asyncio
import logging
import warnings
from typing import Any, Dict, Type, Union

# Third-party Imports
import dill
import multiprocess as mp
from aiodistbus import registry

from chimerapy.engine import config

from ...data_protocols import (
    GatherData,
    NodePubEntry,
    NodePubTable,
    RegisteredMethodData,
    ResultsData,
    WorkerInfo,
)
from ...exceptions import NodeError
from ...logger.zmq_handlers import NodeIDZMQPullListener
from ...networking import DataChunk
from ...node.node_config import NodeConfig
from ...service import Service
from ...states import NodeState, WorkerState
from ...utils import async_waiting_for, update_dataclass
from .context_session import ContextSession, MPSession, ThreadSession
from .node_controller import MPNodeController, NodeController, ThreadNodeController


class NodeHandlerService(Service):
    def __init__(
        self,
        name: str,
        state: WorkerState,
        logger: logging.Logger,
        logreceiver: NodeIDZMQPullListener,
    ):
        super().__init__(name=name)

        # Input parameters
        self.state = state
        self.logger = logger
        self.logreceiver = logreceiver

        # State information
        self.node_controllers: Dict[str, NodeController] = {}
        self.mp_manager = mp.Manager()

        # Map cls to context
        self.context_class_map: Dict[str, Type[NodeController]] = {
            "multiprocessing": MPNodeController,
            "threading": ThreadNodeController,
        }

    @registry.on("start", namespace=f"{__name__}.NodeHandlerService")
    async def start(self) -> bool:
        # Containers
        self.mp_session = MPSession()
        self.thread_session = ThreadSession()
        self.context_session_map: Dict[str, ContextSession] = {
            "multiprocessing": self.mp_session,
            "threading": self.thread_session,
        }
        return True

    @registry.on("shutdown", namespace=f"{__name__}.NodeHandlerService")
    async def shutdown(self) -> bool:

        tasks = [
            self.node_controllers[node_id].shutdown()
            for node_id in self.node_controllers
        ]
        await asyncio.gather(*tasks)

        # Close all the sessions
        self.mp_session.shutdown()
        self.thread_session.shutdown()

        # Clear node_controllers afterwards
        self.node_controllers = {}

        return True

    ###################################################################################
    ## Helper Functions
    ###################################################################################

    def _create_node_pub_table(self) -> NodePubTable:

        # Construct simple data structure for Node to address information
        node_pub_table = NodePubTable()
        for node_id, node_state in self.state.nodes.items():
            node_entry = NodePubEntry(ip=self.state.ip, port=node_state.port)
            node_pub_table.table[node_id] = node_entry

        return node_pub_table

    @registry.on("node.gather", GatherData, namespace=f"{__name__}.NodeHandlerService")
    def update_gather(self, gather_data: GatherData):
        node_id = gather_data.node_id
        self.node_controllers[node_id].gather = gather_data.output
        self.node_controllers[node_id].response = True

    @registry.on(
        "node.registered_method_results",
        ResultsData,
        namespace=f"{__name__}.NodeHandlerService",
    )
    def update_results(self, results_data: ResultsData):
        node_id = results_data.node_id
        self.node_controllers[node_id].registered_method_results = results_data.output
        self.node_controllers[node_id].response = True

    ###################################################################################
    ## Node Handling
    ###################################################################################

    @registry.on("node.status", NodeState, namespace=f"{__name__}.NodeHandlerService")
    async def node_status_update(self, node_state: NodeState):

        # self.logger.debug(f"{self}: received NodeState: {node_state}")

        # Update our records by grabbing all data from the msg
        if node_state.id in self.state.nodes and node_state:

            # Update the node state
            update_dataclass(self.state.nodes[node_state.id], node_state)
            await self.entrypoint.emit("WorkerState.changed", self.state)

    @registry.on("create_node", NodeConfig, namespace=f"{__name__}.NodeHandlerService")
    async def async_create_node(self, node_config: Union[NodeConfig, Dict]):

        # Ensure to convert the node_config into a NodeConfig object
        if isinstance(node_config, dict):
            warnings.warn(
                "node_config parameter as type Dict is going to be deprecated soon",
                DeprecationWarning,
                stacklevel=2,
            )
            node_config = NodeConfig(**node_config)

        # Extract id for ease
        id = node_config.id

        # Saving name to track it for now
        self.logger.debug(
            f"{self}: received request for Node {node_config.id} creation:"
        )

        # Saving the node data
        self.state.nodes[id] = NodeState(id=id)

        # Decode the node object
        node_object = dill.loads(node_config.pickled)

        # Record the node name
        self.state.nodes[id].name = node_object.name

        # Create worker info and set it
        worker_info = WorkerInfo(
            host=self.state.ip,
            port=self.state.port,
            logdir=self.state.tempfolder,
            node_config=node_config,
            config=config.config,
            logging_level=self.logger.level,
            worker_logging_port=self.logreceiver.port,
        )
        node_object.set_worker_info(worker_info)

        # Create controller
        controller = self.context_class_map[node_config.context](
            node_object, self.logger
        )
        if isinstance(controller, MPNodeController):
            controller.set_mp_manager(self.mp_manager)
        controller.run(self.context_session_map[node_config.context])
        self.logger.debug(f"{self}: started {node_object}")

        # Wait until response from node
        success = await async_waiting_for(
            condition=lambda: self.state.nodes[id].fsm in ["INITIALIZED", "READY"],
            timeout=config.get("worker.timeout.node-creation"),
        )

        if not success:
            self.logger.error(f"{self}: Node {id} failed to initialized")
            await controller.shutdown()
            raise NodeError("Node {id} failed to initialized")

        # Now we wait until the node has fully initialized and ready-up
        success = await async_waiting_for(
            condition=lambda: self.state.nodes[id].fsm == "READY",
            timeout=config.get("worker.timeout.info-request"),
        )

        if not success:
            self.logger.error(f"{self}: Node {id} failed to ready-up")
            await controller.shutdown()
            raise NodeError("Node {id} failed to ready-up")

        # Save all important external attributes of the node
        self.node_controllers[node_config.id] = controller

        # Mark success
        # self.logger.debug(f"{self}: completed node creation: {id}")

    @registry.on("destroy_node", str, namespace=f"{__name__}.NodeHandlerService")
    async def async_destroy_node(self, node_id: str):

        if node_id in self.node_controllers:
            # self.logger.debug(f"{self}: destroying Node {node_id}")
            await self.node_controllers[node_id].shutdown()
            # self.logger.debug(f"{self}: destroyed Node {node_id}")

            if node_id in self.state.nodes:
                del self.state.nodes[node_id]

        else:
            self.logger.warning(f"{self}: Node {node_id} does not exist")

    @registry.on(
        "process_node_pub_table",
        NodePubTable,
        namespace=f"{__name__}.NodeHandlerService",
    )
    async def async_process_node_pub_table(self, node_pub_table: NodePubTable):

        await self.entrypoint.emit("worker.node_pub_table", node_pub_table)

        # Now wait until all nodes have responded as CONNECTED
        success = []
        for node_id in self.state.nodes:
            for i in range(config.get("worker.allowed-failures")):
                if await async_waiting_for(
                    condition=lambda: self.state.nodes[node_id].fsm == "CONNECTED",
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    # self.logger.debug(f"{self}: Node {node_id} has connected: SUCCES")
                    success.append(True)
                    break
                else:
                    self.logger.error(f"{self}: Node {node_id} has connected: FAILED")
                    success.append(False)

        if not all(success):
            # self.logger.error(f"{self}: Nodes failed to establish P2P connections")
            raise NodeError("Nodes failed to establish P2P connections")

    @registry.on("start_nodes", namespace=f"{__name__}.NodeHandlerService")
    async def async_start_nodes(self):

        # Send message to nodes to start
        await self.entrypoint.emit("worker.start")
        await async_waiting_for(
            lambda: all(
                [
                    x.fsm in ["PREVIEWING", "RECORDING"]
                    for x in self.state.nodes.values()
                ]
            )
        )

    @registry.on("record_nodes", namespace=f"{__name__}.NodeHandlerService")
    async def async_record_nodes(self):

        # Send message to nodes to record
        await self.entrypoint.emit("worker.record")
        await async_waiting_for(
            lambda: all([x.fsm in ["RECORDING"] for x in self.state.nodes.values()])
        )

    @registry.on("stop_nodes", namespace=f"{__name__}.NodeHandlerService")
    async def async_stop_nodes(self):

        # Send message to nodes to start
        await self.entrypoint.emit("worker.stop")
        await async_waiting_for(
            lambda: all(
                [
                    x.fsm in ["STOPPED", "SAVED", "SHUTDOWN"]
                    for x in self.state.nodes.values()
                ]
            )
        )

    @registry.on(
        "registered_method",
        RegisteredMethodData,
        namespace=f"{__name__}.NodeHandlerService",
    )
    async def async_request_registered_method(
        self, reg_method_data: RegisteredMethodData
    ) -> Dict[str, Any]:

        # Mark that the node hasn't responsed
        self.node_controllers[reg_method_data.node_id].response = False

        await self.entrypoint.emit("worker.registered_method", reg_method_data)

        # Then wait for the Node response
        success = await async_waiting_for(
            condition=lambda: self.node_controllers[reg_method_data.node_id].response
            is True,
        )

        return {
            "success": success,
            "output": self.node_controllers[
                reg_method_data.node_id
            ].registered_method_results,
        }

    @registry.on("diagnostics", bool, namespace=f"{__name__}.NodeHandlerService")
    async def async_diagnostics(self, enable: bool):
        await self.entrypoint.emit("worker.diagnostics", enable)

    @registry.on("gather_nodes", namespace=f"{__name__}.NodeHandlerService")
    async def async_gather(self) -> Dict:

        # self.logger.debug(f"{self}: reporting to Manager gather request")

        for node_id in self.state.nodes:
            self.node_controllers[node_id].response = False

        # Request gather from Worker to Nodes
        await self.entrypoint.emit("worker.gather")

        # Wait until all Nodes have gather
        success = []
        for node_id in self.state.nodes:
            for _ in range(config.get("worker.allowed-failures")):

                if await async_waiting_for(
                    condition=lambda: self.node_controllers[node_id].response is True,
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    # self.logger.debug(
                    #     f"{self}: Node {node_id} responded to gather: SUCCESS"
                    # )
                    success.append(True)
                    break
                else:
                    self.logger.error(
                        f"{self}: Node {node_id} responded to gather: FAILED"
                    )
                    success.append(False)

                if not all(success):
                    self.logger.error(f"{self}: Nodes failed to report to gather")

        # Gather the data from the nodes!
        gather_data: Dict[str, DataChunk] = {}
        for node_id, controller in self.node_controllers.items():
            if controller.gather is None:
                data_chunk = DataChunk()
                data_chunk.add("default", None)
                controller.gather = data_chunk
            gather_data[node_id] = controller.gather

        return gather_data

    @registry.on("collect", namespace=f"{__name__}.NodeHandlerService")
    async def async_collect(self):

        # Request saving from Worker to Nodes
        await self.entrypoint.emit("worker.collect")

        # Now wait until all nodes have responded as CONNECTED
        success = []
        for node_id in self.state.nodes:

            if await async_waiting_for(
                condition=lambda: self.state.nodes[node_id].fsm == "SAVED",
                timeout=None,
            ):
                # self.logger.debug(
                #     f"{self}: Node {node_id} responded to saving request: SUCCESS"
                # )
                success.append(True)
                break
            else:
                # self.logger.error(
                #     f"{self}: Node {node_id} responded to saving request: FAILED"
                # )
                # success.append(False)
                raise NodeError(f"Node {node_id} failed to save")

        if not all(success):
            # self.logger.error(f"{self}: Nodes failed to report to saving")
            raise NodeError("Nodes failed to report to saving")
