from ..conftest import TEST_DATA_DIR, TEST_SAMPLE_DATA_DIR
from ..streams.data_nodes import VideoNode, AudioNode, ImageNode, TabularNode
from ..networking.test_client_server import server

import os
import shutil

import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
cp.debug()


# Constants
assert server
NAME_CLASS_MAP = {
    "vn": VideoNode,
    "img_n": ImageNode,
    "tn": TabularNode,
    "an": AudioNode,
}


def test_worker_instance(worker):
    ...


def test_worker_instance_shutdown_twice(worker):
    worker.shutdown()


def test_send_archive_locally(worker):

    # Adding simple file
    test_file = worker.tempfolder / "test.txt"
    if test_file.exists():
        os.remove(test_file)
    else:
        with open(test_file, "w") as f:
            f.write("hello")

    dst = TEST_DATA_DIR / "test_folder"
    os.makedirs(dst, exist_ok=True)

    new_folder_name = dst / f"{worker.name}-{worker.id}"
    if new_folder_name.exists():
        shutil.rmtree(new_folder_name)

    future = worker._exec_coro(worker.services.http_client._send_archive_locally(dst))
    assert future.result(timeout=5)

    dst_worker = dst / f"{worker.name}-{worker.id}"
    dst_test_file = dst_worker / "test.txt"
    assert dst_worker.exists() and dst_test_file.exists()

    with open(dst_test_file, "r") as f:
        assert f.read() == "hello"


def test_send_archive_remotely(worker, server):

    # Make a copy of example logs
    shutil.copytree(
        str(TEST_SAMPLE_DATA_DIR / "chimerapy_logs"), str(worker.tempfolder / "data")
    )

    future = worker._exec_coro(
        worker.services.http_client._send_archive_remotely(server.host, server.port)
    )
    # future.result(timeout=10)
    future.result()
    logger.debug(server.file_transfer_records)

    # Also check that the file exists
    for record in server.file_transfer_records[worker.id].values():
        assert record["dst_filepath"].exists()
