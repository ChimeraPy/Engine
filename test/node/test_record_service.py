import time
import datetime
import uuid

import numpy as np

import chimerapy as cp
from chimerapy.node.record_service import RecordService
from ..conftest import GenNode, TEST_DATA_DIR

logger = cp._logger.getLogger("chimerapy")
cp.debug()


def test_run_node_in_debug_mode():

    gen_node = GenNode(name="Gen", logdir=TEST_DATA_DIR / "record_service_test")

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
