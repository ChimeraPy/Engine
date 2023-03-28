# Built-in Imports
import time
import json

# Third-party Imports
import pytest
import requests
from pytest_lazyfixture import lazy_fixture

# Interal Imports
import chimerapy as cp
from chimerapy.utils import waiting_for


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

@pytest.mark.parametrize(
    "config_manager",
    [
        (lazy_fixture("manager")),
        (lazy_fixture("single_node_no_connections_manager")),
        (lazy_fixture("multiple_nodes_one_worker_manager")),
        (lazy_fixture("multiple_nodes_multiple_workers_manager")),
    ],
)
def test_post_start(config_manager):

    route = f"http://{config_manager.host}:{config_manager.port}/start"
    r = requests.post(route)
    time.sleep(5)
    assert r.status_code == requests.codes.ok
    assert config_manager.state.running == True
    config_manager.stop()


@pytest.mark.parametrize(
    "config_manager",
    [
        (lazy_fixture("manager")),
        (lazy_fixture("single_node_no_connections_manager")),
        (lazy_fixture("multiple_nodes_one_worker_manager")),
        (lazy_fixture("multiple_nodes_multiple_workers_manager")),
    ],
)
def test_post_stop(config_manager):

    config_manager.start()
    time.sleep(5)

    route = f"http://{config_manager.host}:{config_manager.port}/stop"
    r = requests.post(route)
    assert r.status_code == requests.codes.ok
    assert config_manager.state.running == False


@pytest.mark.parametrize(
    "config_manager, expected_number_of_folders",
    [
        (lazy_fixture("manager"), 0),
        (lazy_fixture("single_node_no_connections_manager"), 1),
        (lazy_fixture("multiple_nodes_one_worker_manager"), 1),
        (lazy_fixture("multiple_nodes_multiple_workers_manager"), 2),
    ],
)
def test_post_collect(config_manager, expected_number_of_folders):

    config_manager.start()
    time.sleep(5)
    config_manager.stop()

    route = f"http://{config_manager.host}:{config_manager.port}/collect"
    r = requests.post(route)
    assert r.status_code == requests.codes.ok
    assert config_manager.state.running == False
    waiting_for(condition=lambda: config_manager.state.collecting == False, timeout=15)

    # Assert the behavior
    assert (
        len([x for x in config_manager.logdir.iterdir() if x.is_dir()])
        == expected_number_of_folders
    )
    assert (config_manager.logdir / "meta.json").exists()

