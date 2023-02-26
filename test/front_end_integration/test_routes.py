# Built-in Imports
import time
import json

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


def test_post_registered_method(
    single_node_with_reg_methods_manager, node_with_reg_methods
):
    single_node_with_reg_methods_manager.start()
    time.sleep(2)

    # The front-end will use HTTP requests to execute registered methods
    r = requests.post(
        f"http://{single_node_with_reg_methods_manager.host}:{single_node_with_reg_methods_manager.port}"
        + "/registered_methods",
        json.dumps(
            {
                "node_id": node_with_reg_methods.id,
                "method_name": "set_value",
                "timeout": 10,
                "params": {"value": 100},
            }
        ),
    )
    assert r.status_code == requests.codes.ok
    msg = r.json()
    assert msg["success"]
    assert msg["return"] == 100

    time.sleep(2)
