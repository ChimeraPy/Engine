import time
import os
import pathlib


import chimerapy.engine as cpe
from ..conftest import TEST_DATA_DIR

logger = cpe._logger.getLogger("chimerapy-engine")

# Constant
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_PACKAGE_DIR = TEST_DIR / "MOCK"


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
