import time
import datetime
import uuid
import os

import numpy as np

import chimerapy.engine as cpe
from chimerapy.engine.node.record_service import RecordService
from ..conftest import GenNode, TEST_DATA_DIR
from ..streams import VideoNode

logger = cpe._logger.getLogger("chimerapy-engine")
cpe.debug()


def test_record_direct_submit():

    # Remove expected file
    expected_file = TEST_DATA_DIR / "record_service_test" / "test.mp4"
    try:
        os.remove(expected_file)
    except Exception:
        ...

    gen_node = GenNode(name="Gen", logdir=TEST_DATA_DIR / "record_service_test")
    gen_node.state.fsm = "RECORDING"

    record_service = RecordService(name="record")
    record_service.inject(gen_node)

    record_service.setup()

    timestamp = datetime.datetime.now()
    video_entry = {
        "uuid": uuid.uuid4(),
        "name": "test",
        "data": np.ndarray([255, 255]),
        "dtype": "video",
        "fps": 30,
        "elapsed": 0,
        "timestamp": timestamp,
    }

    for i in range(50):
        record_service.submit(video_entry)

    record_service.teardown()
    logger.debug("Exited teardown")
    assert expected_file.exists()

    logger.debug("Finished test!")


def test_run_record_service_from_node():

    # Remove expected file
    expected_file = TEST_DATA_DIR / "record_service_test" / "test.mp4"
    try:
        os.remove(expected_file)
    except Exception:
        ...

    node = VideoNode(name="video", logdir=TEST_DATA_DIR / "record_service_test")
    node.run(blocking=False)

    time.sleep(5)

    node.shutdown()
    assert expected_file.exists()


def test_run_record_service_from_worker(worker):

    node = VideoNode(name="video")
    assert worker.create_node(cpe.NodeConfig(node)).result(timeout=10)

    logger.debug("Start nodes!")
    worker.start_nodes().result(timeout=5)
    worker.record_nodes().result(timeout=5)

    logger.debug("Let nodes run for some time")
    time.sleep(5)
    worker.stop_nodes().result(timeout=5)

    assert worker.collect().result(timeout=10)
    path = worker.state.tempfolder / node.name / "test.mp4"
    logger.debug(path)
    assert (path).exists()
