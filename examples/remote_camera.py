from typing import Dict, Any
import time
import pathlib
import os

import numpy as np
import cv2

import chimerapy as cp

CWD = pathlib.Path(os.path.abspath(__file__)).parent


class WebcamNode(cp.Node):
    def prep(self):
        self.vid = cv2.VideoCapture(0)

    def step(self):
        time.sleep(1 / 30)
        ret, frame = self.vid.read()
        return frame

    def teardown(self):
        self.vid.release()


class ShowWindow(cp.Node):
    def step(self, data: Dict[str, Any]):

        frame = data["web"]
        if not isinstance(frame, np.ndarray):
            return

        cv2.imshow("frame", frame)
        cv2.waitKey(1)

    def teardown(self):
        cv2.destroyAllWindows()


class RemoteCameraGraph(cp.Graph):
    def __init__(self):
        super().__init__()
        web = WebcamNode(name="web")
        show = ShowWindow(name="show")

        self.add_nodes_from([web, show])
        self.add_edge(src=web, dst=show)


if __name__ == "__main__":

    # Create default manager and desired graph
    manager = cp.Manager(logdir=CWD / "runs")
    graph = RemoteCameraGraph()
    worker = cp.Worker(name="local")

    # Then register graph to Manager
    worker.connect(host=manager.host, port=manager.port)

    # Wait until workers connect
    while True:
        q = input("All workers connected? (Y/n)")
        if q.lower() == "y":
            break

    # Assuming one worker
    mapping = {"remote": ["web"], "local": ["show"]}
    # mapping = {"local": ["web", "show"]}

    # Commit the graph
    manager.commit_graph(graph=graph, mapping=mapping, timeout=10)

    # Wail until user stops
    while True:
        q = input("Ready to start? (Y/n)")
        if q.lower() == "y":
            break

    manager.start()

    # Wail until user stops
    while True:
        q = input("Stop? (Y/n)")
        if q.lower() == "y":
            break

    manager.stop()
    manager.shutdown()
