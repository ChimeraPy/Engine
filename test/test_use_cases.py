from typing import Dict, Any
import time
import logging
import os
import sys
import multiprocessing as mp

import mss
import cv2
import numpy as np
import imutils
import pytest
from pytest_lazyfixture import lazy_fixture

from chimerapy import Node, Graph

logger = logging.getLogger("chimerapy")


class WebcamNode(Node):
    def prep(self):
        self.vid = cv2.VideoCapture(0)

    def step(self):
        time.sleep(1 / 30)
        ret, frame = self.vid.read()
        return imutils.resize(frame, width=400)

    def teardown(self):
        self.vid.release()


class ScreenCapture(Node):
    def prep(self):

        # https://stackoverflow.com/questions/8257385/automatic-detection-of-display-availability-with-matplotlib
        if "DISPLAY" not in os.environ:
            self.monitor = None
        else:
            self.sct = mss.mss()
            self.monitor = self.sct.monitors[0]

    def step(self):
        time.sleep(1 / 10)
        if self.monitor:
            frame = np.array(self.sct.grab(self.monitor), dtype=np.uint8)
            return imutils.resize(frame, width=400)
        else:
            return np.ones((400, 400))


class ShowWindow(Node):
    def step(self, data: Dict[str, Any]):

        # time.sleep(1/20)

        if "web" in data:
            frame = data["web"]
        elif "screen" in data:
            frame = data["screen"]
        else:
            return None

        # cv2.imshow("frame", frame)
        # cv2.waitKey(1)

    # def teardown(self):
    #     cv2.destroyAllWindows()


class CombineAndShow(Node):
    def step(self, data: Dict[str, Any]):

        web_frame = data["web"]
        screen_frame = data["screen"][..., :3]

        dim = (screen_frame.shape[1], screen_frame.shape[0])

        web_frame = cv2.resize(web_frame, dim, interpolation=cv2.INTER_AREA)
        frame = np.concatenate((web_frame, screen_frame), axis=0)

        # cv2.imshow("frame", frame)
        # cv2.waitKey(1)

    # def teardown(self):
    #     cv2.destroyAllWindows()


@pytest.fixture
def webcam_graph():

    web = WebcamNode(name="web")
    show = ShowWindow(name="show")

    graph = Graph()
    graph.add_nodes_from([web, show])
    graph.add_edge(src=web, dst=show)

    return graph


@pytest.fixture
def screencapture_graph():

    screen = ScreenCapture(name="screen")
    show = ShowWindow(name="show")

    graph = Graph()
    graph.add_nodes_from([screen, show])
    graph.add_edge(src=screen, dst=show)

    return graph


@pytest.fixture
def combine_videos_graph():

    web = WebcamNode(name="web")
    screen = ScreenCapture(name="screen")
    combine = CombineAndShow(name="combine")

    graph = Graph()
    graph.add_nodes_from([screen, web, combine])
    graph.add_edge(src=screen, dst=combine)
    graph.add_edge(src=web, dst=combine)

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
        (lazy_fixture("webcam_graph"), {"local": ["web", "show"]}),
        (lazy_fixture("screencapture_graph"), {"local": ["screen", "show"]}),
        (lazy_fixture("combine_videos_graph"), {"local": ["screen", "combine", "web"]}),
    ],
)
def test_use_case_graph(manager, worker, graph, mapping):

    # Connect to the manager
    worker.connect(host=manager.host, port=manager.port)

    # Then register graph to Manager
    manager.register_graph(graph)

    # Specify what nodes to what worker
    manager.map_graph(mapping)

    manager.commit_graph()
    manager.wait_until_all_nodes_ready(timeout=10)

    # Take a single step and see if the system crashes and burns
    manager.start()
    time.sleep(5)
    manager.stop()
