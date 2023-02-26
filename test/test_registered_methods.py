import time
import json

import requests
import dill
import pytest

import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
cp.debug()


@pytest.fixture
def worker_with_reg(worker, node_with_reg_methods):

    # Simple single node without connection
    msg = {
        "id": node_with_reg_methods.id,
        "pickled": dill.dumps(node_with_reg_methods),
        "in_bound": [],
        "in_bound_by_name": [],
        "out_bound": [],
        "follow": None,
    }

    logger.debug("Create nodes")
    worker.create_node(msg)

    logger.debug("Waiting before starting!")
    time.sleep(2)

    logger.debug("Start nodes!")
    worker.start_nodes()

    logger.debug("Let nodes run for some time")
    time.sleep(5)

    return worker


def test_registered_method_with_concurrent_style(
    worker_with_reg, node_with_reg_methods
):

    # Execute the registered method (with config)
    r = requests.post(
        f"http://{worker_with_reg.ip}:{worker_with_reg.port}"
        + "/nodes/registered_methods",
        json.dumps(
            {
                "node_id": node_with_reg_methods.id,
                "method_name": "printout",
                "timeout": 10,
                "params": {},
            }
        ),
    )
    assert r.status_code == requests.codes.ok
    msg = r.json()
    assert msg["success"] and isinstance(msg["return"], int)

    # Make sure the system works after the request
    time.sleep(2)


def test_registered_method_with_params_and_blocking_style(
    worker_with_reg, node_with_reg_methods
):

    # Execute the registered method (with config)
    r = requests.post(
        f"http://{worker_with_reg.ip}:{worker_with_reg.port}"
        + "/nodes/registered_methods",
        json.dumps(
            {
                "node_id": node_with_reg_methods.id,
                "method_name": "set_value",
                "timeout": 10,
                "params": {"value": -100},
            }
        ),
    )
    assert r.status_code == requests.codes.ok
    msg = r.json()
    assert msg["success"] and msg["return"] == -100

    # Make sure the system works after the request
    time.sleep(2)


def test_registered_method_with_reset_style(worker_with_reg, node_with_reg_methods):

    # Execute the registered method (without config and params)
    r = requests.post(
        f"http://{worker_with_reg.ip}:{worker_with_reg.port}"
        + "/nodes/registered_methods",
        json.dumps(
            {
                "node_id": node_with_reg_methods.id,
                "method_name": "reset",
                "timeout": 10,
                "params": {},
            }
        ),
    )
    assert r.status_code == requests.codes.ok
    msg = r.json()
    assert msg["success"] and msg["return"] == 100

    # Make sure the system works after the request
    time.sleep(2)


def test_manager_requesting_registered_methods(
    single_node_with_reg_methods_manager, node_with_reg_methods
):
    single_node_with_reg_methods_manager.start()
    time.sleep(2)
    assert single_node_with_reg_methods_manager.request_registered_method(
        node_id=node_with_reg_methods.id, method_name="printout", timeout=10
    )
    time.sleep(2)
    assert single_node_with_reg_methods_manager.request_registered_method(
        node_id=node_with_reg_methods.id,
        method_name="set_value",
        timeout=10,
        params={"value": -100},
    )
    time.sleep(2)
    assert single_node_with_reg_methods_manager.request_registered_method(
        node_id=node_with_reg_methods.id, method_name="reset", timeout=10
    )
    time.sleep(2)
