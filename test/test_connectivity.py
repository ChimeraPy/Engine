import time

import pytest

import chimerapy as cp


def test_manager_instance(manager):
    ...


def test_worker_instance(worker):
    ...


@pytest.mark.xfail(reason="Incorrect port")
def test_worker_connect_to_incorrect_address(manager, worker):
    worker.connect(host=manager.host, port=manager.port + 1)


def test_manager_registering_worker_locally(manager, worker):
    worker.connect(host=manager.host, port=manager.port)
    assert worker.name in manager.workers


def test_manager_registering_workers_locally(manager):

    workers = []
    for name in ["local", "local2", "local3"]:
        worker = cp.Worker(name=name)
        worker.connect(host=manager.host, port=manager.port)
        workers.append(worker)

    for worker in workers:
        assert worker.name in manager.workers
        worker.shutdown()


def test_manager_shutting_down_workers_after_delay(manager):

    workers = []
    for name in ["local", "local2", "local3"]:
        worker = cp.Worker(name=name)
        worker.connect(host=manager.host, port=manager.port)
        workers.append(worker)

    time.sleep(1)

    for worker in workers:
        assert worker.name in manager.workers
        worker.shutdown()


def test_manager_shutting_down_workers_to_close_all():

    # Create the actors
    manager = cp.Manager()
    worker = cp.Worker(name="local")

    # Connect to the Manager
    worker.connect(host=manager.host, port=manager.port)

    # Wait and then shutdown system through the manager
    manager.shutdown()
