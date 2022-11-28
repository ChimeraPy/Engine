from typing import Dict, Any
import time

import cv2
import imutils

import chimerapy as cp


class Producer(cp.Node):
    def prep(self):
        self.counter = 0

    def step(self):
        time.sleep(1)
        current_counter = self.counter
        self.counter += 1
        return current_counter


class Consumer(cp.Node):
    def step(self, data: Dict[str, Any]):
        d = data["prod"]
        print("Consumer got data: ", d)


class SimpleGraph(cp.Graph):
    def __init__(self):
        super().__init__()
        prod = Producer(name="prod")
        cons = Consumer(name="cons")

        self.add_nodes_from([prod, cons])
        self.add_edge(src=prod, dst=cons)


if __name__ == "__main__":

    # Create default manager and desired graph
    manager = cp.Manager()
    graph = SimpleGraph()
    worker = cp.Worker(name="local")
    # worker2 = cp.Worker(name="remote")

    # Then register graph to Manager
    worker.connect(host=manager.host, port=manager.port)
    # worker2.connect(host=manager.host, port=manager.port)
    manager.register_graph(graph)

    # Wait until workers connect
    while True:
        q = input("All workers connected? (Y/n)")
        if q.lower() == "y":
            break

    # Assuming one worker
    mapping = {"remote": ["prod"], "local": ["cons"]}

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
    manager.shutdown()
