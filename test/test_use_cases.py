from typing import Dict, Any
import time
import logging
import os
import sys
import multiprocessing as mp

from PIL import ImageGrab
import cv2
import numpy as np
import imutils
import pytest
from pytest_lazyfixture import lazy_fixture

import chimerapy as cp

logger = cp._logger.getLogger("chimerapy")
# cp.debug(["chimerapy-networking", "chimerapy-subprocess"])
cp.debug()


class WebcamNode(cp.Node):
    def prep(self):
        self.vid = cv2.VideoCapture(0)

    def step(self) -> cp.DataChunk:
        time.sleep(1 / 10)
        ret, frame = self.vid.read()
        data_chunk = cp.DataChunk()
        data_chunk.add("frame", frame, "image")
        return data_chunk

    def teardown(self):
        self.vid.release()


class ScreenCapture(cp.Node):
    def prep(self):

        # https://stackoverflow.com/questions/8257385/automatic-detection-of-display-availability-with-matplotlib
        if "DISPLAY" not in os.environ:
            self.monitor = None
        else:
            self.monitor = True

    def step(self) -> cp.DataChunk:

        time.sleep(1 / 10)
        if self.monitor:
            frame = cv2.cvtColor(
                np.array(ImageGrab.grab(), dtype=np.uint8), cv2.COLOR_RGB2BGR
            )
        else:
            frame = (np.random.rand(1000, 1000, 3) * 255).astype(np.uint8)

        # Create container and send it
        data_chunk = cp.DataChunk()
        data_chunk.add("frame", frame, "image")
        return data_chunk


class ShowWindow(cp.Node):
    def step(self, data_chunks: Dict[str, cp.DataChunk]):

        for name, data_chunk in data_chunks.items():
            self.logger.debug(f"{self}: got from {name}, data={data_chunk}")

            cv2.imshow(name, data_chunk.get("frame")["value"])
            cv2.waitKey(1)


@pytest.fixture
def webcam_graph():

    web = WebcamNode(name="web")
    show = ShowWindow(name="show")

    graph = cp.Graph()
    graph.add_nodes_from([web, show])
    graph.add_edge(src=web, dst=show)

    return graph


@pytest.fixture
def screencapture_graph():

    screen = ScreenCapture(name="screen")
    show = ShowWindow(name="show")

    graph = cp.Graph()
    graph.add_nodes_from([screen, show])
    graph.add_edge(src=screen, dst=show)

    return graph


@pytest.fixture
def show_multiple_videos_graph():

    web = WebcamNode(name="web")
    screen = ScreenCapture(name="screen")
    show = ShowWindow(name="show")

    graph = cp.Graph()
    graph.add_nodes_from([screen, web, show])
    graph.add_edge(src=screen, dst=show)
    graph.add_edge(src=web, dst=show)

    return graph


@pytest.mark.skipif(
    sys.platform == "darwin", reason="Camera restrictions that require GUI"
)
def test_open_camera():
    cap = cv2.VideoCapture(0)
    ret, frame = cap.read()
    assert isinstance(ret, bool)


@pytest.mark.skipif(
    sys.platform == "darwin", reason="Camera restrictions that require GUI"
)
def test_open_camera_in_another_process():

    p = mp.Process(target=test_open_camera)
    p.start()
    p.join()


@pytest.mark.skipif(
    sys.platform == "darwin", reason="Camera restrictions that require GUI"
)
@pytest.mark.parametrize(
    "graph, mapping",
    [
        # (lazy_fixture("webcam_graph"), {"local": ["web", "show"]}),
        # (lazy_fixture("screencapture_graph"), {"local": ["screen", "show"]}),
        (
            lazy_fixture("show_multiple_videos_graph"),
            {"local": ["screen", "show", "web"]},
        ),
    ],
)
def test_use_case_graph(manager, worker, graph, mapping):

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    assert manager.commit_graph(graph=graph, mapping=mapping)

    # Take a single step and see if the system crashes and burns
    manager.start()
    time.sleep(5)
    manager.stop()
