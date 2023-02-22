# Built-in Imports
import logging
import subprocess
import time
import sys

# Third-party Imports
import pytest
import chimerapy as cp

# Test Import
from .mock import DockeredWorker
from .conftest import linux_run_only

logger = cp._logger.getLogger("chimerapy")
cp.debug()


@linux_run_only
def test_worker_entrypoint_connect(manager, dockered_worker):

    # Create docker container to simulate Worker computer
    dockered_worker.connect(manager.host, manager.port)
    logger.info("Executed cmd to connect Worker to Manager.")

    time.sleep(2)

    # Assert that the Worker is connected
    assert dockered_worker.id in manager.workers
    logger.info("Manager shutting down")
    manager.shutdown()


def test_worker_entrypoint_connect_wport(manager):

    # Connect to manager from subprocess
    worker_process = subprocess.Popen(
        [
            "cp-worker",
            "--name",
            "test",
            "--id",
            "test",
            "--ip",
            manager.host,
            "--port",
            str(manager.port),
            "--wport",
            str(9980),
        ]
    )
    logger.info("Executed cmd to connect Worker to Manager.")

    time.sleep(2)
    assert "test" in manager.workers
    assert manager.workers["test"].port == 9980

    logger.info("Killing worker subprocess")
    worker_process.kill()

    logger.info("Manager shutting down")
    manager.shutdown()


@linux_run_only
def test_multiple_workers_connect(manager, docker_client):

    workers = []
    for i in range(10):
        worker = DockeredWorker(docker_client, name=f"test-{i}")
        worker.connect(manager.host, manager.port)
        workers.append(worker)

    for worker in workers:
        assert worker.id in manager.workers

    logger.info("Manager shutting down")
    manager.shutdown()

    time.sleep(1)

    for worker in workers:
        worker.shutdown()
