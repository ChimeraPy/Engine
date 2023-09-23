# Test Import

# Built-in Imports
import subprocess
import time

# Third-party Imports
import chimerapy.engine as cpe

from .conftest import TEST_DATA_DIR

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


def test_worker_entrypoint_connect_wport():

    manager = cpe.Manager(name="test", port=0, logdir=TEST_DATA_DIR)
    manager.serve()

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


def test_worker_entrypoint_zeroconf_connect():

    manager = cpe.Manager(name="test", port=0, logdir=TEST_DATA_DIR)
    manager.serve()

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
