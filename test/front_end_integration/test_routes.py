# Built-in Imports

# Third-party Imports
import pytest
import requests
from pytest_lazyfixture import lazy_fixture

# Interal Imports
import chimerapy as cp


@pytest.mark.parametrize(
    "config_manager",
    [
        (lazy_fixture("manager")),
        (lazy_fixture("single_node_no_connections_manager")),
        (lazy_fixture("multiple_nodes_one_worker_manager")),
        (lazy_fixture("multiple_nodes_multiple_workers_manager")),
    ],
)
def test_get_network(config_manager):

    route = f"http://{config_manager.host}:{config_manager.port}/network"
    r = requests.get(route)
    assert r.status_code == requests.codes.ok
    assert r.json() == config_manager.state.to_dict()
