from typing import Dict, Any
import time

import cv2
import imutils

import chimerapy as cp


class WebcamNode(cp.Node):
    def prep(self):
        self.vid = cv2.VideoCapture(0)

    def step(self):
        time.sleep(1 / 30)
        ret, frame = self.vid.read()
        return imutils.resize(frame, width=400)

    def teardown(self):
        self.vid.release()


class ShowWindow(cp.Node):
    def step(self, data: Dict[str, Any]):
        frame = data["web"]
        cv2.imshow("frame", frame)
        cv2.waitKey(1)


class RemoteCameraGraph(cp.Graph):
    def __init__(self):
        super().__init__()
        web = WebcamNode(name="web")
        show = ShowWindow(name="show")

        self.add_nodes_from([web, show])
        self.add_edge(src=web, dst=show)


if __name__ == "__main__":

    # Create default manager and desired graph
    manager = cp.Manager()
    graph = RemoteCameraGraph()

    # Then register graph to Manager
    manager.register_graph(graph)

    # Wait until workers connect
    while True:
        q = input("All workers connected? (Y/n)")
        if q.lower() == "y":
            break

    # Assuming one worker
    worker_name = list(manager.workers.keys())[0]
    mapping = {worker_name: ["web", "show"]}

    # Specify what nodes to what worker
    manager.map_graph(mapping)

    # Commit the graph
    manager.commit_graph()
    manager.wait_until_all_nodes_ready(timeout=10)

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
