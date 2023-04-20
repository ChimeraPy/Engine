from typing import Dict, Any
import time
import pathlib
import os

import chimerapy as cp

CWD = pathlib.Path(os.path.abspath(__file__)).parent


class Producer(cp.Node):
    def setup(self):
        self.counter = 0

    def step(self):
        time.sleep(1)
        current_counter = self.counter
        self.counter += 1
        data_chunk = cp.DataChunk()
        data_chunk.add("counter", current_counter)
        return data_chunk


class Consumer(cp.Node):
    def step(self, data: Dict[str, Any]):
        d = data["prod"].get("counter")["value"]
        print("Consumer got data: ", d)


class SimpleGraph(cp.Graph):
    def __init__(self):
        super().__init__()
        self.prod = Producer(name="prod")
        self.cons = Consumer(name="cons")

        self.add_nodes_from([self.prod, self.cons])
        self.add_edge(src=self.prod, dst=self.cons)
        self.node_ids = [self.prod.id, self.cons.id]


if __name__ == "__main__":

    # Create default manager and desired graph
    manager = cp.Manager(logdir=CWD / "runs")
    graph = SimpleGraph()
    worker = cp.Worker(name="local")

    # Then register graph to Manager
    worker.connect(host=manager.host, port=manager.port)

    # Wait until workers connect
    while True:
        q = input("All workers connected? (Y/n)")
        if q.lower() == "y":
            break

    # Assuming one worker
    # mapping = {worker.id: graph.node_ids}
    # mapping = {worker.id: [graph.prod.id], 'remote': [graph.cons.id]}
    mapping = {worker.id: [graph.cons.id], "remote": [graph.prod.id]}

    # Commit the graph
    manager.commit_graph(graph=graph, mapping=mapping).result(timeout=60)

    # Wail until user stops
    while True:
        q = input("Ready to start? (Y/n)")
        if q.lower() == "y":
            break

    manager.start().result(timeout=5)

    # Wail until user stops
    while True:
        q = input("Stop? (Y/n)")
        if q.lower() == "y":
            break

    manager.stop().result(timeout=5)
    manager.shutdown()
