from typing import Dict, Any
import time
import pathlib
import os

import numpy as np
import cv2
import mss
import imutils

import chimerapy as cp

cp.debug()

CWD = pathlib.Path(os.path.abspath(__file__)).parent


class WebcamNode(cp.Node):
    def prep(self):
        self.vid = cv2.VideoCapture(0)

    def step(self):
        time.sleep(1 / 15)
        ret, frame = self.vid.read()
        self.save_video(name="test", data=frame, fps=20)
        return imutils.resize(frame, width=600)

    def teardown(self):
        self.vid.release()


class ScreenCaptureNode(cp.Node):
    def prep(self):
        self.sct = mss.mss()
        self.monitor = self.sct.monitors[0]

    def step(self):
        time.sleep(1 / 30)
        frame = np.array(self.sct.grab(self.monitor), dtype=np.uint8)[..., :3]
        self.save_video(name="screen", data=frame, fps=20)
        return imutils.resize(frame, width=600)


class ShowWindow(cp.Node):
    def step(self, data: Dict[str, Any]):

        for name, value in data.items():
            cv2.imshow(name, imutils.resize(value, width=1000))
        cv2.waitKey(1)

    def teardown(self):
        cv2.destroyAllWindows()


class RemoteCameraGraph(cp.Graph):
    def __init__(self):
        super().__init__()
        web = WebcamNode(name="web")
        screen = ScreenCaptureNode(name="screen")
        show = ShowWindow(name="show")

        self.add_nodes_from([web, show, screen])
        self.add_edges_from([(web, show), (screen, show)])


if __name__ == "__main__":

    # Create default manager and desired graph
    manager = cp.Manager(logdir=CWD / "runs")
    worker = cp.Worker(name="local")

    # Then register graph to Manager
    worker.connect(host=manager.host, port=manager.port)

    # Wait until workers connect
    while True:
        q = input("All workers connected? (Y/n)")
        if q.lower() == "y":
            break

    # For local only
    if len(manager.workers) == 1:
        graph = RemoteCameraGraph()
        mapping = {"local": ["web", "show", "screen"]}
    else:

        # For mutliple workers (remote and local)
        graph = cp.Graph()
        show_node = ShowWindow(name=f"local-show")
        graph.add_node(show_node)
        mapping = {"local": ["local-show"]}

        for worker_name in manager.workers:
            if worker_name == "local":
                continue
            else:
                web_node = WebcamNode(name=f"{worker_name}-web")
                screen_node = ScreenCaptureNode(name=f"{worker_name}-screen")
                graph.add_nodes_from([web_node, screen_node])
                graph.add_edges_from([(web_node, show_node), (screen_node, show_node)])
                mapping[worker_name] = [f"{worker_name}-web", f"{worker_name}-screen"]

    # Commit the graph
    manager.commit_graph(graph=graph, mapping=mapping)

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
    manager.collect()
    manager.shutdown()
