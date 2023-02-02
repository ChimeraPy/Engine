# Built-in Imports
import time
import pathlib
import os

# Third-party
import pytest

# Internal
import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
cp.debug()

# Constants
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent
TEST_DATA_DIR = TEST_DIR / "data"


def test_manager_instance(manager):
    ...


def test_manager_instance_shutdown_twice(manager):
    manager.shutdown()


def test_worker_instance(worker):
    ...


def test_worker_instance_shutdown_twice(worker):
    worker.shutdown()


@pytest.mark.xfail(reason="Incorrect port")
def test_worker_connect_to_incorrect_address(manager, worker):
    worker.connect(host=manager.host, port=manager.port + 1)


def test_manager_registering_worker_locally(manager, worker):
    worker.connect(host=manager.host, port=manager.port)
    assert worker.name in manager.workers


def test_manager_registering_workers_locally(manager):

    workers = []
    for i in range(5):
        worker = cp.Worker(name=f"local-{i}", port=9080 + i * 10)
        worker.connect(host=manager.host, port=manager.port)
        workers.append(worker)

    for worker in workers:
        assert worker.name in manager.workers
        worker.shutdown()


def test_manager_shutting_down_workers_after_delay(manager):

    workers = []
    for i in range(5):
        worker = cp.Worker(name=f"local-{i}", port=9080 + i * 10)
        worker.connect(host=manager.host, port=manager.port)
        workers.append(worker)

    time.sleep(1)

    for worker in workers:
        assert worker.name in manager.workers
        worker.shutdown()


def test_manager_shutting_down_gracefully():
    # While this one should not!

    # Create the actors
    manager = cp.Manager(logdir=TEST_DATA_DIR, port=0)
    worker = cp.Worker(name="local")

    # Connect to the Manager
    worker.connect(host=manager.host, port=manager.port)

    # Wait and then shutdown system through the manager
    worker.shutdown()
    manager.shutdown()


def test_manager_shutting_down_ungracefully():
    # While this one should not!

    # Create the actors
    manager = cp.Manager(logdir=TEST_DATA_DIR, port=0)
    worker = cp.Worker(name="local")

    # Connect to the Manager
    worker.connect(host=manager.host, port=manager.port)

    # Only shutting Manager
    manager.shutdown()
    worker.shutdown()
