from typing import Dict

import pytest
from pytest_lazyfixture import lazy_fixture

import chimerapy_engine as cpe

from .p2p_networking_verifier import P2PNetworkingVerifier

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


@pytest.mark.slow
@pytest.mark.timeout(600)  # Give upto 10 minutes to complete
@pytest.mark.parametrize(
    "config_manager,expected_output",
    [
        (lazy_fixture("single_node_no_connections_manager"), {"Gen1": 2}),
        (lazy_fixture("multiple_nodes_one_worker_manager"), {"Gen1": 2, "Con1": 6}),
        pytest.param(
            lazy_fixture("multiple_nodes_multiple_workers_manager"),
            {"Gen1": 2, "Con1": 6},
            marks=pytest.mark.skip,
        ),
        (lazy_fixture("slow_single_node_single_worker_manager"), {"Slo1": 5}),
        pytest.param(
            lazy_fixture("dockered_single_node_no_connections_manager"),
            {"Gen1": 2},
            marks=pytest.mark.skip,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_one_worker_manager"),
            {"Gen1": 2, "Con1": 6},
            marks=pytest.mark.skip,
        ),
        pytest.param(
            lazy_fixture("dockered_multiple_nodes_multiple_workers_manager"),
            {"Gen1": 2, "Con1": 6},
            marks=pytest.mark.skip,
        ),
    ],
)
def test_p2p_networking(config_manager: cpe.Manager, expected_output: Dict[str, int]):
    p2p_verifier = P2PNetworkingVerifier(config_manager)
    p2p_verifier.assert_nodes_are_ready()
    logger.info("Verified nodes are ready")
    # p2p_verifier.assert_can_step_after_graph_commit(expected_output)
    # logger.info("Verified can step after graph commit")
    p2p_verifier.assert_can_start_and_stop(expected_output)
    logger.info("Verified can start stop nodes after graph commit")
