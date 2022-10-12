import time

import pytest

from chimerapy import Manager, Worker


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
        worker = Worker(name=name)
        worker.connect(host=manager.host, port=manager.port)
        workers.append(worker)

    for worker in workers:
        assert worker.name in manager.workers
        worker.shutdown()


def test_manager_shutting_down_workers_after_delay(manager):

    workers = []
    for name in ["local", "local2", "local3"]:
        worker = Worker(name=name)
        worker.connect(host=manager.host, port=manager.port)
        workers.append(worker)

    time.sleep(1)

    for worker in workers:
        assert worker.name in manager.workers
        worker.shutdown()


# def test_manager_registering_workers_distributed(manager):
#     local_worker = Worker(name="local")
#     local_worker.connect(host=manager.host, port=manager.port)

#     create_and_connect_virtual_worker(
#         machine_name="test_ubuntu", host=manager.host, port=manager.port
#     )

#     manager.setup()
