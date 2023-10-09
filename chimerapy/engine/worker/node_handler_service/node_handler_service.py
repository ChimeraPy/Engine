import warnings
import logging
import asyncio
from typing import Dict, Any, Union, Type

# Third-party Imports
import dill
import multiprocess as mp

from chimerapy.engine import config
from ...logger.zmq_handlers import NodeIDZMQPullListener
from ...service import Service
from ...node.node_config import NodeConfig
from ...data_protocols import NodePubTable
from ...node.worker_comms_service import WorkerCommsService
from ...states import NodeState, WorkerState
from ...networking import DataChunk
from ...networking.enums import WORKER_MESSAGE
from ...utils import async_waiting_for
from ...eventbus import EventBus, TypedObserver, Event
from ..events import (
    EnableDiagnosticsEvent,
    BroadcastEvent,
    SendMessageEvent,
    CreateNodeEvent,
    DestroyNodeEvent,
    ProcessNodePubTableEvent,
    RegisteredMethodEvent,
    UpdateResultsEvent,
    UpdateGatherEvent,
)
from .node_controller import NodeController, ThreadNodeController, MPNodeController
from .context_session import MPSession, ThreadSession, ContextSession


class NodeHandlerService(Service):
    def __init__(
        self,
        name: str,
        state: WorkerState,
        eventbus: EventBus,
        logger: logging.Logger,
        logreceiver: NodeIDZMQPullListener,
    ):
        super().__init__(name=name)

        # Input parameters
        self.state = state
        self.eventbus = eventbus
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

    async def async_init(self):

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "start": TypedObserver("start", on_asend=self.start, handle_event="drop"),
            "shutdown": TypedObserver(
                "shutdown", on_asend=self.shutdown, handle_event="drop"
            ),
            "create_node": TypedObserver(
                "create_node",
                CreateNodeEvent,
                on_asend=self.async_create_node,
                handle_event="unpack",
            ),
            "destroy_node": TypedObserver(
                "destroy_node",
                DestroyNodeEvent,
                on_asend=self.async_destroy_node,
                handle_event="unpack",
            ),
            "process_node_pub_table": TypedObserver(
                "process_node_pub_table",
                ProcessNodePubTableEvent,
                on_asend=self.async_process_node_pub_table,
                handle_event="unpack",
            ),
            "step_nodes": TypedObserver(
                "step_nodes", on_asend=self.async_step, handle_event="drop"
            ),
            "start_nodes": TypedObserver(
                "start_nodes", on_asend=self.async_start_nodes, handle_event="drop"
            ),
            "stop_nodes": TypedObserver(
                "stop_nodes", on_asend=self.async_stop_nodes, handle_event="drop"
            ),
            "record_nodes": TypedObserver(
                "record_nodes", on_asend=self.async_record_nodes, handle_event="drop"
            ),
            "registered_method": TypedObserver(
                "registered_method",
                RegisteredMethodEvent,
                on_asend=self.async_request_registered_method,
                handle_event="unpack",
            ),
            "collect": TypedObserver(
                "collect", on_asend=self.async_collect, handle_event="drop"
            ),
            "gather_nodes": TypedObserver(
                "gather_nodes", on_asend=self.async_gather, handle_event="drop"
            ),
            "diagnostics": TypedObserver(
                "diagnostics",
                EnableDiagnosticsEvent,
                on_asend=self.async_diagnostics,
                handle_event="unpack",
            ),
            "update_gather": TypedObserver(
                "update_gather",
                UpdateGatherEvent,
                on_asend=self.update_gather,
                handle_event="unpack",
            ),
            "update_results": TypedObserver(
                "update_results",
                UpdateResultsEvent,
                on_asend=self.update_results,
                handle_event="unpack",
            ),
        }
        for ob in self.observers.values():
            await self.eventbus.asubscribe(ob)

    async def start(self) -> bool:
        # Containers
        self.mp_session = MPSession()
        self.thread_session = ThreadSession()
        self.context_session_map: Dict[str, ContextSession] = {
            "multiprocessing": self.mp_session,
            "threading": self.thread_session,
        }
        return True

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

    def update_gather(self, node_id: str, gather: Any):
        self.node_controllers[node_id].gather = gather
        self.node_controllers[node_id].response = True

    def update_results(self, node_id: str, results: Any):
        self.node_controllers[node_id].registered_method_results = results
        self.node_controllers[node_id].response = True

    ###################################################################################
    ## Node Handling
    ###################################################################################

    async def async_create_node(self, node_config: Union[NodeConfig, Dict]) -> bool:

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
        # self.logger.debug(
        #     f"{self}: received request for Node {node_config.id} creation:"
        # )

        # Saving the node data
        self.state.nodes[id] = NodeState(id=id)

        # Keep trying to start a process until success
        success = False
        for i in range(config.get("worker.allowed-failures")):

            # Decode the node object
            node_object = dill.loads(node_config.pickled)

            # Record the node name
            self.state.nodes[id].name = node_object.name

            # Create worker service and inject to the Node
            worker_comms = WorkerCommsService(
                name="worker",
                host=self.state.ip,
                port=self.state.port,
                worker_logdir=self.state.tempfolder,
                worker_config=config.config,
                node_config=node_config,
                logging_level=self.logger.level,
                worker_logging_port=self.logreceiver.port,
            )
            # worker_comms.inject(node_object)
            node_object.add_worker_comms(worker_comms)

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
                continue

            # Now we wait until the node has fully initialized and ready-up
            success = await async_waiting_for(
                condition=lambda: self.state.nodes[id].fsm == "READY",
                timeout=config.get("worker.timeout.info-request"),
            )

            if not success:
                self.logger.error(f"{self}: Node {id} failed to ready-up")
                await controller.shutdown()
                continue

            # Save all important external attributes of the node
            self.node_controllers[node_config.id] = controller

            # Mark success
            self.logger.debug(f"{self}: completed node creation: {id}")
            break

        if not success:
            self.logger.error(f"{self}: Node {id} failed to create")
            return False

        return success

    async def async_destroy_node(self, node_id: str) -> bool:

        self.logger.debug(f"{self}: received request for Node {node_id} destruction")
        success = False

        if node_id in self.node_controllers:
            self.logger.debug(f"{self}: destroying Node {node_id}")
            await self.node_controllers[node_id].shutdown()
            self.logger.debug(f"{self}: destroyed Node {node_id}")

            if node_id in self.state.nodes:
                del self.state.nodes[node_id]

            success = True

        return success

    async def async_process_node_pub_table(self, node_pub_table: NodePubTable) -> bool:

        await self.eventbus.asend(
            Event(
                "broadcast",
                BroadcastEvent(
                    signal=WORKER_MESSAGE.BROADCAST_NODE_SERVER,
                    data=node_pub_table.to_dict(),
                ),
            )
        )

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
                    self.logger.debug(f"{self}: Node {node_id} has connected: FAILED")
                    success.append(False)

        if not all(success):
            self.logger.error(f"{self}: Nodes failed to establish P2P connections")

        return all(success)

    async def async_start_nodes(self) -> bool:
        # Send message to nodes to start
        await self.eventbus.asend(
            Event("broadcast", BroadcastEvent(signal=WORKER_MESSAGE.START_NODES))
        )
        return True

    async def async_record_nodes(self) -> bool:
        # Send message to nodes to start
        await self.eventbus.asend(
            Event("broadcast", BroadcastEvent(signal=WORKER_MESSAGE.RECORD_NODES))
        )
        return True

    async def async_step(self) -> bool:
        # Worker tell all nodes to take a step
        await self.eventbus.asend(
            Event("broadcast", BroadcastEvent(signal=WORKER_MESSAGE.REQUEST_STEP))
        )
        return True

    async def async_stop_nodes(self) -> bool:
        # Send message to nodes to start
        await self.eventbus.asend(
            Event("broadcast", BroadcastEvent(signal=WORKER_MESSAGE.STOP_NODES))
        )
        await async_waiting_for(
            lambda: all(
                [
                    x.fsm in ["STOPPED", "SAVED", "SHUTDOWN"]
                    for x in self.state.nodes.values()
                ]
            )
        )
        return True

    async def async_request_registered_method(
        self, node_id: str, method_name: str, params: Dict = {}
    ) -> Dict[str, Any]:

        # Mark that the node hasn't responsed
        self.node_controllers[node_id].response = False
        self.logger.debug(
            f"{self}: Requesting registered method: {method_name}@{node_id}"
        )

        event_data = SendMessageEvent(
            client_id=node_id,
            signal=WORKER_MESSAGE.REQUEST_METHOD,
            data={"method_name": method_name, "params": params},
        )
        await self.eventbus.asend(Event("send", event_data))

        # Then wait for the Node response
        success = await async_waiting_for(
            condition=lambda: self.node_controllers[node_id].response is True,
        )

        return {
            "success": success,
            "output": self.node_controllers[node_id].registered_method_results,
        }

    async def async_diagnostics(self, enable: bool) -> bool:
        await self.eventbus.asend(
            Event(
                "broadcast",
                BroadcastEvent(
                    signal=WORKER_MESSAGE.DIAGNOSTICS, data={"enable": enable}
                ),
            )
        )
        return True

    async def async_gather(self) -> Dict:

        # self.logger.debug(f"{self}: reporting to Manager gather request")

        for node_id in self.state.nodes:
            self.node_controllers[node_id].response = False

        # Request gather from Worker to Nodes
        await self.eventbus.asend(
            Event("broadcast", BroadcastEvent(signal=WORKER_MESSAGE.REQUEST_GATHER))
        )

        # Wait until all Nodes have gather
        success = []
        for node_id in self.state.nodes:
            for i in range(config.get("worker.allowed-failures")):

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
                    self.logger.debug(
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

    async def async_collect(self) -> bool:

        # Request saving from Worker to Nodes
        await self.eventbus.asend(
            Event("broadcast", BroadcastEvent(signal=WORKER_MESSAGE.REQUEST_COLLECT))
        )

        # Now wait until all nodes have responded as CONNECTED
        success = []
        for i in range(config.get("worker.allowed-failures")):
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
                    self.logger.debug(
                        f"{self}: Node {node_id} responded to saving request: FAILED"
                    )
                    success.append(False)

        if not all(success):
            self.logger.error(f"{self}: Nodes failed to report to saving")

        return all(success)
