# Built-in Imports
import logging
import time
import sys

# Third-party Imports
import pytest

# Test Import
from .mock import DockeredWorker
from .conftest import linux_run_only

logger = logging.getLogger("chimerapy")


@linux_run_only
def test_worker_entrypoint_connect(manager, dockered_worker):

    # Create docker container to simulate Worker computer
    dockered_worker.connect(manager.host, manager.port)
    logger.info("Executed cmd to connect Worker to Manager.")

    # Assert that the Worker is connected
    assert dockered_worker.name in manager.workers
    manager.shutdown()
    logger.info("Manager shutting down")


@linux_run_only
def test_multiple_workers_connect(manager, docker_client):

    workers = []
    for i in range(10):
        worker = DockeredWorker(docker_client, name=f"test-{i}")
        worker.connect(manager.host, manager.port)
        workers.append(worker)

    for worker in workers:
        assert worker.name in manager.workers

    manager.shutdown()

    time.sleep(1)

    for worker in workers:
        worker.shutdown()
