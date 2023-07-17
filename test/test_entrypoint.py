# Test Import
from .mock import DockeredWorker

# Built-in Imports
import subprocess
import time

# Third-party Imports
import pytest
import chimerapy.engine as cpe

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


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


def test_worker_entrypoint_zeroconf_connect(manager):

    manager.zeroconf(enable=True)

    # Connect to manager from subprocess
    worker_process = subprocess.Popen(
        [
            "cp-worker",
            "--name",
            "test",
            "--id",
            "test",
            "--zeroconf",
            "true",
        ]
    )
    logger.info("Executed cmd to connect Worker to Manager.")

    time.sleep(5)
    assert "test" in manager.workers

    logger.info("Killing worker subprocess")
    worker_process.kill()

    logger.info("Disable Zeroconf")
    manager.zeroconf(enable=False)


@pytest.mark.skip
def test_multiple_workers_connect(manager, docker_client):

    workers = []
    for i in range(3):
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
