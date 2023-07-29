import threading
import typing
import warnings
import logging
from typing import Dict, Any, Union, Type, Optional

if typing.TYPE_CHECKING:
    from ..node.node import Node

# Third-party Imports
import dill
import multiprocess as mp

from chimerapy.engine import config
from ..logger.zmq_handlers import NodeIDZMQPullListener
from ..service import Service
from ..node.node_config import NodeConfig
from ..node.worker_comms_service import WorkerCommsService
from ..states import NodeState, WorkerState
from ..networking import DataChunk
from ..networking.enums import WORKER_MESSAGE
from ..utils import async_waiting_for
from ..eventbus import EventBus, TypedObserver, Event


class NodeController:
    node_object: "Node"

    context: Union[threading.Thread, mp.Process]

    response: bool = False
    gather: DataChunk = DataChunk()
    registered_method_results: Any = None

    def __init__(self, node_object: "Node"):

        # Save parameters
        self.node_object = node_object

    def start(self):
        self.context.start()

    def stop(self):
        ...

    def shutdown(self, timeout: Optional[Union[int, float]] = None):
        ...


class ThreadNodeController(NodeController):

    context: threading.Thread
    running: bool

    def __init__(self, node_object: "Node"):
        super().__init__(node_object)

        # Create a thread to run the Node
        self.context = threading.Thread(target=self.node_object.run, args=(True,))

    def stop(self):
        self.node_object.running = False

    def shutdown(self, timeout: Optional[Union[int, float]] = None):
        self.stop()
        self.context.join(timeout=timeout)


class MPNodeController(NodeController):

    context: mp.Process
    running: mp.Value

    def __init__(self, node_object: "Node"):
        super().__init__(node_object)

        # Create a process to run the Node
        self.running = mp.Value("i", True)
        self.context = mp.Process(
            target=self.node_object.run,
            args=(
                True,
                self.running,
            ),
        )

    def stop(self):
        self.running.value = False

    def shutdown(self, timeout: Optional[Union[int, float]] = None):
        self.stop()
        self.context.join(timeout=timeout)
        self.context.terminate()


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

        # Containers
        self.node_controllers: Dict[str, NodeController] = {}

        # Map cls to context
        self.context_class_map: Dict[str, Type[NodeController]] = {
            "multiprocessing": MPNodeController,
            "threading": ThreadNodeController,
        }

        # Specify observers
        self.observers: Dict[str, TypedObserver] = {
            "shutdown": TypedObserver(
                "shutdown", on_asend=self.shutdown, handle_event="drop"
            ),
            "create_node": TypedObserver(
                "create_node", on_asend=self.async_create_node, handle_event="unpack"
            ),
        }
        for ob in self.observers.values():
            self.eventbus.subscribe(ob).result(timeout=1)

    async def shutdown(self) -> bool:

        # Shutdown nodes from the client (start all shutdown)
        for node_id in self.node_controllers:
            self.node_controllers[node_id].stop()

        # Then wait until close, or force
        for node_id in self.node_controllers:
            self.node_controllers[node_id].shutdown(
                timeout=config.get("worker.timeout.node-shutdown")
            )

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
        self.logger.debug(
            f"{self}: received request for Node {node_config.id} creation:"
        )

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
            worker_service = WorkerCommsService(
                name="worker",
                host=self.state.ip,
                port=self.state.port,
                worker_logdir=self.state.tempfolder,
                node_config=node_config,
                logging_level=self.logger.level,
                worker_logging_port=self.logreceiver.port,
            )
            worker_service.inject(node_object)

            # Create controller
            controller = self.context_class_map[node_config.context](node_object)

            # Start the node
            controller.start()
            self.logger.debug(f"{self}: started {node_object}")

            # Wait until response from node
            success = await async_waiting_for(
                condition=lambda: self.state.nodes[id].fsm in ["INITIALIZED", "READY"],
                timeout=config.get("worker.timeout.node-creation"),
            )

            if not success:
                controller.shutdown()
                continue

            # Now we wait until the node has fully initialized and ready-up
            success = await async_waiting_for(
                condition=lambda: self.state.nodes[id].fsm == "READY",
                timeout=config.get("worker.timeout.info-request"),
            )

            if not success:
                controller.shutdown()
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
            self.node_controllers[node_id].shutdown()

            if node_id in self.state.nodes:
                del self.state.nodes[node_id]

            success = True

        return success

    async def async_process_node_server_data(self, msg: Dict) -> bool:

        await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.BROADCAST_NODE_SERVER,
            data=msg,
        )

        # Now wait until all nodes have responded as CONNECTED
        success = []
        for node_id in self.state.nodes:
            for i in range(config.get("worker.allowed-failures")):
                if await async_waiting_for(
                    condition=lambda: self.state.nodes[node_id].fsm == "CONNECTED",
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    self.logger.debug(f"{self}: Nodes {node_id} has connected: PASS")
                    success.append(True)
                    break
                else:
                    self.logger.debug(f"{self}: Node {node_id} has connected: FAIL")
                    success.append(False)

        if not all(success):
            self.logger.error(f"{self}: Nodes failed to establish P2P connections")

        return all(success)

    async def async_start_nodes(self) -> bool:
        # Send message to nodes to start
        await self.eventbus.asend(Event("start_nodes"))
        # return await self.worker.services["http_server"]._async_broadcast(
        #     signal=WORKER_MESSAGE.START_NODES, data={}
        # )
        return True

    async def async_record_nodes(self) -> bool:
        # Send message to nodes to start
        return await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.RECORD_NODES, data={}
        )

    async def async_step(self) -> bool:
        # Worker tell all nodes to take a step
        return await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.REQUEST_STEP, data={}
        )

    async def async_stop_nodes(self) -> bool:
        # Send message to nodes to start
        await self.eventbus.asend(Event("stop_nodes"))
        # return await self.worker.services["http_server"]._async_broadcast(
        #     signal=WORKER_MESSAGE.STOP_NODES, data={}
        # )
        return True

    async def async_request_registered_method(
        self, node_id: str, method_name: str, params: Dict = {}
    ) -> Dict[str, Any]:

        # Mark that the node hasn't responsed
        self.node_controllers[node_id].response = False
        self.logger.debug(
            f"{self}: Requesting registered method: {method_name}@{node_id}"
        )

        success = True

        await self.worker.services["http_server"]._async_send(
            client_id=node_id,
            signal=WORKER_MESSAGE.REQUEST_METHOD,
            data={"method_name": method_name, "params": params},
        )

        # Then wait for the Node response
        success = await async_waiting_for(
            condition=lambda: self.node_controllers[node_id].response is True,
        )

        return {
            "success": success,
            "output": self.node_controllers[node_id].registered_method_results,
        }

    async def async_gather(self) -> Dict:

        self.logger.debug(f"{self}: reporting to Manager gather request")

        for node_id in self.state.nodes:
            self.node_controllers[node_id].response = False

        # Request gather from Worker to Nodes
        await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.REQUEST_GATHER, data={}
        )

        # Wait until all Nodes have gather
        success = []
        for node_id in self.state.nodes:
            for i in range(config.get("worker.allowed-failures")):

                if await async_waiting_for(
                    condition=lambda: self.node_controllers[node_id].response is True,
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    self.logger.debug(
                        f"{self}: Node {node_id} responded to gather: PASS"
                    )
                    success.append(True)
                    break
                else:
                    self.logger.debug(
                        f"{self}: Node {node_id} responded to gather: FAIL"
                    )
                    success.append(False)

                if not all(success):
                    self.logger.error(f"{self}: Nodes failed to report to gather")

        # Gather the data from the nodes!
        gather_data = {"id": self.state.id, "node_data": {}}
        for node_id, controller in self.node_controllers.items():
            if controller.gather is None:
                data_chunk = DataChunk()
                data_chunk.add("default", None)
                controller.gather = data_chunk
            gather_data["node_data"][node_id] = controller.gather

        return gather_data

    async def async_collect(self) -> bool:

        # Request saving from Worker to Nodes
        await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.REQUEST_COLLECT, data={}
        )

        # Now wait until all nodes have responded as CONNECTED
        success = []
        for i in range(config.get("worker.allowed-failures")):
            for node_id in self.state.nodes:

                if await async_waiting_for(
                    condition=lambda: self.state.nodes[node_id].fsm == "SAVED",
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    self.logger.debug(
                        f"{self}: Node {node_id} responded to saving request: PASS"
                    )
                    success.append(True)
                    break
                else:
                    self.logger.debug(
                        f"{self}: Node {node_id} responded to saving request: FAIL"
                    )
                    success.append(False)

        if not all(success):
            self.logger.error(f"{self}: Nodes failed to report to saving")

        return all(success)
