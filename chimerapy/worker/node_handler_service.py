import collections
from typing import Dict, Any

# Third-party Imports
import dill
import multiprocess as mp

from chimerapy import config
from .worker_service import WorkerService
from ..node.worker_comms_service import WorkerCommsService
from ..states import NodeState
from ..networking import DataChunk
from ..networking.enums import WORKER_MESSAGE
from ..utils import async_waiting_for


class NodeHandlerService(WorkerService):
    def __init__(self, name: str):
        super().__init__(name=name)
        self.nodes_extra = collections.defaultdict(dict)

    async def shutdown(self) -> bool:

        # Shutdown nodes from the client (start all shutdown)
        for node_id in self.nodes_extra:
            self.nodes_extra[node_id]["running"].value = False

        # Then wait until close, or force
        for node_id in self.nodes_extra:
            self.nodes_extra[node_id]["process"].join(
                timeout=config.get("worker.timeout.node-shutdown")
            )

            # If that doesn't work, terminate
            if self.nodes_extra[node_id]["process"].exitcode != 0:
                self.worker.logger.warning(f"{self}: Node {node_id} forced shutdown")
                self.nodes_extra[node_id]["process"].terminate()

            self.worker.logger.debug(f"{self}: Nodes have joined")

        # Clear nodes_extra afterwards
        self.nodes_extra = {}

        return True

    ###################################################################################
    ## Helper Functions
    ###################################################################################

    def update_gather(self, node_id: str, gather: Any):
        self.nodes_extra[node_id]["gather"] = gather
        self.nodes_extra[node_id]["response"] = True

    def update_results(self, node_id: str, results: Any):
        self.nodes_extra[node_id]["registered_method_results"] = results
        self.nodes_extra[node_id]["response"] = True

    ###################################################################################
    ## Node Handling
    ###################################################################################

    async def async_create_node(self, node_id: str, msg: Dict) -> bool:

        # Saving name to track it for now
        self.worker.logger.debug(
            f"{self}: received request for Node {node_id} creation:"
        )

        # Saving the node data
        self.worker.state.nodes[node_id] = NodeState(id=node_id)
        self.nodes_extra[node_id]["response"] = False
        self.nodes_extra[node_id]["gather"] = DataChunk()
        self.nodes_extra[node_id]["registered_method_results"] = None
        self.nodes_extra[node_id].update({k: v for k, v in msg.items() if k != "id"})
        self.worker.logger.debug(f"{self}: created state for <Node {node_id}>")

        # Keep trying to start a process until success
        success = False
        for i in range(config.get("worker.allowed-failures")):

            # Decode the node object
            self.nodes_extra[node_id]["node_object"] = dill.loads(
                self.nodes_extra[node_id]["pickled"]
            )
            self.worker.logger.debug(f"{self}: unpickled <Node {node_id}>")

            # Record the node name
            self.worker.state.nodes[node_id].name = self.nodes_extra[node_id][
                "node_object"
            ].name

            # Create worker service and inject to the Node
            running = mp.Value("i", True)
            worker_service = WorkerCommsService(
                name="worker",
                host=self.worker.state.ip,
                port=self.worker.state.port,
                worker_logdir=self.worker.tempfolder,
                in_bound=self.nodes_extra[node_id]["in_bound"],
                in_bound_by_name=self.nodes_extra[node_id]["in_bound_by_name"],
                out_bound=self.nodes_extra[node_id]["out_bound"],
                follow=self.nodes_extra[node_id]["follow"],
                logging_level=self.worker.logger.level,
                worker_logging_port=self.worker.logreceiver.port,
            )
            worker_service.inject(self.nodes_extra[node_id]["node_object"])
            self.worker.logger.debug(
                f"{self}: injected {self.nodes_extra[node_id]['node_object']} with \
                WorkerCommsService"
            )

            # Create a process to run the Node
            process = mp.Process(
                target=self.nodes_extra[node_id]["node_object"].run,
                args=(
                    True,
                    running,
                ),
            )
            self.nodes_extra[node_id]["process"] = process
            self.nodes_extra[node_id]["running"] = running

            # Start the node
            process.start()
            self.worker.logger.debug(f"{self}: started <Node {node_id}>")

            # Wait until response from node
            success = await async_waiting_for(
                condition=lambda: self.worker.state.nodes[node_id].fsm
                in ["INITIALIZED", "READY"],
                timeout=config.get("worker.timeout.node-creation"),
            )

            if success:
                self.worker.logger.debug(f"{self}: {node_id} responding, SUCCESS")
            else:
                # Handle failure
                self.worker.logger.debug(f"{self}: {node_id} responding, FAILED, retry")
                self.nodes_extra[node_id]["running"].value = False
                self.nodes_extra[node_id]["process"].join()
                self.nodes_extra[node_id]["process"].terminate()
                continue

            self.worker.logger.debug(f"{self}: {self.worker.state}")

            # Now we wait until the node has fully initialized and ready-up
            success = await async_waiting_for(
                condition=lambda: self.worker.state.nodes[node_id].fsm == "READY",
                timeout=config.get("worker.timeout.info-request"),
            )

            if success:
                self.worker.logger.debug(f"{self}: {node_id} fully ready, SUCCESS")
                break
            else:
                # Handle failure
                self.worker.logger.debug(
                    f"{self}: {node_id} fully ready, FAILED, retry"
                )
                self.nodes_extra[node_id]["running"].value = False
                self.nodes_extra[node_id]["process"].join()
                self.nodes_extra[node_id]["process"].terminate()

        if not success:
            self.worker.logger.error(f"{self}: Node {node_id} failed to create")
        else:
            # Mark success
            self.worker.logger.debug(f"{self}: completed node creation: {node_id}")

        return success

    async def async_destroy_node(self, node_id: str) -> bool:

        self.worker.logger.debug(
            f"{self}: received request for Node {node_id} destruction"
        )
        success = False

        if node_id in self.nodes_extra:
            self.nodes_extra[node_id]["running"].value = False
            self.nodes_extra[node_id]["process"].join(
                timeout=config.get("worker.timeout.node-shutdown")
            )

            # If that doesn't work, terminate
            if self.nodes_extra[node_id]["process"].exitcode != 0:
                self.worker.logger.warning(f"{self}: Node {node_id} forced shutdown")
                self.nodes_extra[node_id]["process"].terminate()

            if node_id in self.worker.state.nodes:
                del self.worker.state.nodes[node_id]

            success = True

        return success

    async def async_process_node_server_data(self, msg: Dict) -> bool:

        await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.BROADCAST_NODE_SERVER,
            data=msg,
        )

        # Now wait until all nodes have responded as CONNECTED
        success = []
        for node_id in self.worker.state.nodes:
            for i in range(config.get("worker.allowed-failures")):
                if await async_waiting_for(
                    condition=lambda: self.worker.state.nodes[node_id].fsm
                    == "CONNECTED",
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    self.worker.logger.debug(
                        f"{self}: Nodes {node_id} has connected: PASS"
                    )
                    success.append(True)
                    break
                else:
                    self.worker.logger.debug(
                        f"{self}: Node {node_id} has connected: FAIL"
                    )
                    success.append(False)

        if not all(success):
            self.worker.logger.error(
                f"{self}: Nodes failed to establish P2P connections"
            )

        return all(success)

    async def async_start_nodes(self) -> bool:
        # Send message to nodes to start
        return await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.START_NODES, data={}
        )

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
        return await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.STOP_NODES, data={}
        )

    async def async_request_registered_method(
        self, node_id: str, method_name: str, params: Dict = {}
    ) -> Dict[str, Any]:

        # Mark that the node hasn't responsed
        self.nodes_extra[node_id]["response"] = False
        self.worker.logger.debug(
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
            condition=lambda: self.nodes_extra[node_id]["response"] is True,
        )

        return {
            "success": success,
            "output": self.nodes_extra[node_id]["registered_method_results"],
        }

    async def async_gather(self) -> Dict:

        self.worker.logger.debug(f"{self}: reporting to Manager gather request")

        for node_id in self.worker.state.nodes:
            self.nodes_extra[node_id]["response"] = False

        # Request gather from Worker to Nodes
        await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.REQUEST_GATHER, data={}
        )

        # Wait until all Nodes have gather
        success = []
        for node_id in self.worker.state.nodes:
            for i in range(config.get("worker.allowed-failures")):

                if await async_waiting_for(
                    condition=lambda: self.nodes_extra[node_id]["response"] is True,
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    self.worker.logger.debug(
                        f"{self}: Node {node_id} responded to gather: PASS"
                    )
                    success.append(True)
                    break
                else:
                    self.worker.logger.debug(
                        f"{self}: Node {node_id} responded to gather: FAIL"
                    )
                    success.append(False)

                if not all(success):
                    self.worker.logger.error(
                        f"{self}: Nodes failed to report to gather"
                    )

        # Gather the data from the nodes!
        gather_data = {"id": self.worker.state.id, "node_data": {}}
        for node_id, node_data in self.nodes_extra.items():
            if node_data["gather"] is None:
                data_chunk = DataChunk()
                data_chunk.add("default", None)
                node_data["gather"] = data_chunk
            gather_data["node_data"][node_id] = node_data["gather"]

        return gather_data

    async def async_collect(self) -> bool:

        # Request saving from Worker to Nodes
        await self.worker.services["http_server"]._async_broadcast(
            signal=WORKER_MESSAGE.REQUEST_COLLECT, data={}
        )

        # Now wait until all nodes have responded as CONNECTED
        success = []
        for i in range(config.get("worker.allowed-failures")):
            for node_id in self.worker.nodes:

                if await async_waiting_for(
                    condition=lambda: self.worker.state.nodes[node_id].fsm == "SAVED",
                    timeout=config.get("worker.timeout.info-request"),
                ):
                    self.worker.logger.debug(
                        f"{self}: Node {node_id} responded to saving request: PASS"
                    )
                    success.append(True)
                    break
                else:
                    self.worker.logger.debug(
                        f"{self}: Node {node_id} responded to saving request: FAIL"
                    )
                    success.append(False)

        if not all(success):
            self.worker.logger.error(f"{self}: Nodes failed to report to saving")

        return all(success)
