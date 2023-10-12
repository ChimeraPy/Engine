import os
import pathlib
import time

import chimerapy.engine as cpe

from ..conftest import TEST_DATA_DIR

logger = cpe._logger.getLogger("chimerapy-engine")

# Constant
TEST_DIR = pathlib.Path(os.path.abspath(__file__)).parent.parent
TEST_PACKAGE_DIR = TEST_DIR / "MOCK"


async def test_manager_instance(manager):
    ...


async def test_manager_instance_shutdown_twice(manager):
    await manager.async_shutdown()


async def test_manager_registering_worker_locally(manager, worker):
    await worker.async_connect(host=manager.host, port=manager.port)
    assert worker.id in manager.workers


def test_sync_manager_registering_worker_locally():
    manager = cpe.Manager(logdir=TEST_DATA_DIR, port=0)
    manager.serve()
    worker = cpe.Worker(name="local", port=0)
    worker.serve()
    worker.connect(host=manager.host, port=manager.port)
    assert worker.id in manager.workers


async def test_manager_registering_via_localhost(manager, worker):
    await worker.async_connect(host="localhost", port=manager.port)
    assert worker.id in manager.workers


async def test_manager_registering_workers_locally(manager):

    workers = []
    for i in range(3):
        worker = cpe.Worker(name=f"local-{i}", port=0)
        await worker.aserve()
        await worker.async_connect(method="ip", host=manager.host, port=manager.port)
        workers.append(worker)

    time.sleep(1)

    for worker in workers:
        assert worker.id in manager.workers
        await worker.async_shutdown()


async def test_zeroconf_connect(manager, worker):

    await manager.async_zeroconf(enable=True)

    await worker.async_connect(method="zeroconf")
    assert worker.id in manager.workers

    await manager.async_zeroconf(enable=False)


async def test_manager_shutting_down_gracefully():

    # Create the actors
    manager = cpe.Manager(logdir=TEST_DATA_DIR, port=0)
    await manager.aserve()
    worker = cpe.Worker(name="local", port=0)
    await worker.aserve()

    # Connect to the Manager
    await worker.async_connect(method="ip", host=manager.host, port=manager.port)

    # Wait and then shutdown system through the manager
    await worker.async_shutdown()
    await manager.async_shutdown()


async def test_manager_shutting_down_ungracefully():

    # Create the actors
    manager = cpe.Manager(logdir=TEST_DATA_DIR, port=0)
    await manager.aserve()
    worker = cpe.Worker(name="local", port=0)
    await worker.aserve()

    # Connect to the Manager
    await worker.async_connect(method="ip", host=manager.host, port=manager.port)

    # Only shutting Manager
    await manager.async_shutdown()
    await worker.async_shutdown()
