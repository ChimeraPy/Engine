import time
from typing import Dict

import chimerapy as cp


class P2PNetworkingVerifier:
    """Verify that the p2p networking is working as expected in chimerapy.

    Note: This is a refactor of the test_p2p_networking.py in
    https://github.com/oele-isis-vanderbilt/ChimeraPy/blob/e47b4cd3056db08f4406b7eb91b25e815931eeea/test/networking/test_p2p_networking.py
    """

    def __init__(self, manager: cp.Manager):
        self.manager = manager

    def assert_nodes_are_ready(self) -> None:
        worker_node_map = self.manager.worker_graph_map

        nodes_ids = []
        for worker_id in self.manager.workers:

            # Assert that the worker has their expected nodes
            expected_nodes = worker_node_map[worker_id]
            union = set(expected_nodes) | set(
                self.manager.workers[worker_id].nodes.keys()
            )
            assert len(union) == len(expected_nodes)

            # Assert that all the nodes should be INIT
            for node_id in (worker_nodes := self.manager.workers[worker_id].nodes):
                assert worker_nodes[node_id].init
                nodes_ids.append(node_id)

        # The manager should have all the nodes are registered
        assert all([self.manager.graph.has_node_by_id(x) for x in nodes_ids])

        # Extract all the nodes
        nodes_ids = []
        for worker_id in self.manager.workers:
            for node_id in (worker_nodes := self.manager.workers[worker_id].nodes):
                assert worker_nodes[node_id].connected
                nodes_ids.append(node_id)

        # The manager should have all the nodes registered as CONNECTED
        assert all([x in self.manager.nodes_server_table for x in nodes_ids])

        # Extract all the nodes
        nodes_idss = []
        for worker_id in self.manager.workers:
            for node_id in (worker_nodes := self.manager.workers[worker_id].nodes):
                assert worker_nodes[node_id].ready
                nodes_idss.append(node_id)

        # The manager should have all the nodes registered as READY
        assert all([x in self.manager.nodes_server_table for x in nodes_idss])

    def assert_can_step_after_graph_commit(
        self, expected_output: Dict[str, int]
    ) -> None:
        # Take a single step and see if the system crashes and burns
        self.manager.step()
        time.sleep(5)

        # Then request gather and confirm that the data is valid
        latest_data_values = self.manager.gather()

        # Convert the expected from name to id
        expected_output_by_id = {}
        for k, v in expected_output.items():
            id = self.manager.graph.get_id_by_name(k)
            expected_output_by_id[id] = v

        # Assert
        for k, v in expected_output_by_id.items():
            assert (
                k in latest_data_values
                and isinstance(latest_data_values[k], cp.DataChunk)
                and latest_data_values[k].get("default")["value"] == v
            )

    def assert_can_start_and_stop(self, expected_output: Dict[str, int]) -> None:
        # Take a single step and see if the system crashes and burns
        self.manager.start()
        time.sleep(5)
        self.manager.stop()

        # Then request gather and confirm that the data is valid
        latest_data_values = self.manager.gather()

        # Convert the expected from name to id
        expected_output_by_id = {}
        for k, v in expected_output.items():
            id = self.manager.graph.get_id_by_name(k)
            expected_output_by_id[id] = v

        # Assert
        for k, v in expected_output_by_id.items():
            assert (
                k in latest_data_values
                and isinstance(latest_data_values[k], cp.DataChunk)
                and latest_data_values[k].get("default")["value"] == v
            )
