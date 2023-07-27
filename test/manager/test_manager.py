import time
import os
import pathlib

import pytest
from pytest_lazyfixture import lazy_fixture

import chimerapy.engine as cpe
from ..conftest import GenNode, ConsumeNode, TEST_DATA_DIR, linux_run_only

logger = cpe._logger.getLogger("chimerapy-engine")

# Constant
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_PACKAGE_DIR = TEST_DIR / "mock"


@pytest.fixture
def local_node_graph(gen_node):
    graph = cpe.Graph()
    graph.add_node(gen_node)
    return graph


@pytest.fixture
def packaged_node_graph():
    # Installing test package (in case this test package hasn't been done before)
    # Reference: https://stackoverflow.com/a/55188705/13231446
    try:
        import test_package as tp
    except ImportError:
        import pip._internal as pip

        pip.main(["install", str(TEST_PACKAGE_DIR)])
    finally:
        import test_package as tp

    node = tp.TestNode(name="test")
    graph = cpe.Graph()
    graph.add_node(node)
    return graph


def test_manager_instance(manager):
    ...


def test_manager_instance_shutdown_twice(manager):
    manager.shutdown()


def test_manager_registering_worker_locally(manager, worker):
    worker.connect(host=manager.host, port=manager.port)
    assert worker.id in manager.workers


def test_manager_registering_via_localhost(manager, worker):
    worker.connect(host="localhost", port=manager.port)
    assert worker.id in manager.workers


def test_manager_registering_workers_locally(manager):

    workers = []
    for i in range(3):
        worker = cpe.Worker(name=f"local-{i}", port=0)
        worker.connect(method="ip", host=manager.host, port=manager.port)
        workers.append(worker)

    time.sleep(1)

    for worker in workers:
        assert worker.id in manager.workers
        worker.shutdown()


def test_zeroconf_connect(manager, worker):

    manager.zeroconf(enable=True)

    worker.connect(method="zeroconf", blocking=False).result(timeout=30)
    assert worker.id in manager.workers

    manager.zeroconf(enable=False)


def test_manager_shutting_down_gracefully():

    # Create the actors
    manager = cpe.Manager(logdir=TEST_DATA_DIR, port=0)
    worker = cpe.Worker(name="local", port=0)

    # Connect to the Manager
    worker.connect(method="ip", host=manager.host, port=manager.port)

    # Wait and then shutdown system through the manager
    worker.shutdown()
    manager.shutdown()


def test_manager_shutting_down_ungracefully():

    # Create the actors
    manager = cpe.Manager(logdir=TEST_DATA_DIR, port=0)
    worker = cpe.Worker(name="local", port=0)

    # Connect to the Manager
    worker.connect(method="ip", host=manager.host, port=manager.port)

    # Only shutting Manager
    manager.shutdown()
    worker.shutdown()


@pytest.mark.skip(reason="Frozen test")
@linux_run_only
@pytest.mark.parametrize(
    "_worker, config_graph",
    [
        (lazy_fixture("worker"), lazy_fixture("local_node_graph")),
        (lazy_fixture("worker"), lazy_fixture("packaged_node_graph")),
        pytest.param(
            lazy_fixture("dockered_worker"),
            lazy_fixture("local_node_graph"),
            marks=pytest.mark.skip,
        ),
        pytest.param(
            lazy_fixture("dockered_worker"),
            lazy_fixture("packaged_node_graph"),
            marks=pytest.mark.skip,
        ),
    ],
)
def test_sending_package(manager, _worker, config_graph):
    _worker.connect(host=manager.host, port=manager.port)

    assert manager.commit_graph(
        graph=config_graph,
        mapping={_worker.id: list(config_graph.G.nodes())},
        send_packages=[
            {"name": "test_package", "path": TEST_PACKAGE_DIR / "test_package"}
        ],
    ).result(timeout=30)

    for node_id in config_graph.G.nodes():
        assert manager.workers[_worker.id].nodes[node_id].fsm != "NULL"


@pytest.mark.parametrize("context", ["multiprocessing", "threading"])
def test_manager_lifecycle(manager, worker, context):

    # Define graph
    gen_node = GenNode(name="Gen1")
    con_node = ConsumeNode(name="Con1")
    graph = cpe.Graph()
    graph.add_nodes_from([gen_node, con_node])
    graph.add_edge(src=gen_node, dst=con_node)

    # Configure the worker and obtain the mapping
    worker.connect(host=manager.host, port=manager.port)
    mapping = {worker.id: [gen_node.id, con_node.id]}
    manager.commit_graph(graph, mapping, context=context).result(timeout=30)

    assert manager.start().result()

    time.sleep(3)

    assert manager.record().result()

    time.sleep(3)

    assert manager.stop().result()
    assert manager.collect().result()


def test_manager_reset(manager, worker):

    # Define graph
    gen_node = GenNode(name="Gen1")
    con_node = ConsumeNode(name="Con1")
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from([gen_node, con_node])
    simple_graph.add_edge(src=gen_node, dst=con_node)

    # Configure the worker and obtain the mapping
    worker.connect(host=manager.host, port=manager.port)
    mapping = {worker.id: [gen_node.id, con_node.id]}

    manager.reset(keep_workers=True).result(timeout=30)

    manager.commit_graph(graph=simple_graph, mapping=mapping).result(timeout=30)
    assert manager.start().result()

    time.sleep(3)

    assert manager.record().result()

    time.sleep(3)

    assert manager.stop().result()
    assert manager.collect().result()


@pytest.mark.skip(reason="Flaky")
def test_manager_recommit_graph(worker, manager):

    # Define graph
    gen_node = GenNode(name="Gen1")
    con_node = ConsumeNode(name="Con1")
    simple_graph = cpe.Graph()
    simple_graph.add_nodes_from([gen_node, con_node])
    simple_graph.add_edge(src=gen_node, dst=con_node)

    # Configure the worker and obtain the mapping
    worker.connect(host=manager.host, port=manager.port)
    mapping = {worker.id: [gen_node.id, con_node.id]}

    graph_info = {"graph": simple_graph, "mapping": mapping}

    logger.debug("STARTING COMMIT 1st ROUND")
    tic = time.time()
    assert manager.commit_graph(**graph_info).result(timeout=30)
    toc = time.time()
    delta = toc - tic
    logger.debug("FINISHED COMMIT 1st ROUND")

    logger.debug("STARTING RESET")
    assert manager.reset(keep_workers=True).result(timeout=30)
    logger.debug("FINISHED RESET")

    logger.debug("STARTING COMMIT 2st ROUND")
    tic2 = time.time()
    assert manager.commit_graph(**graph_info).result(timeout=30)
    toc2 = time.time()
    delta2 = toc2 - tic2
    logger.debug("FINISHED COMMIT 2st ROUND")

    assert ((delta2 - delta) / (delta)) < 1
