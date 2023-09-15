from typing import Dict
import time
import pathlib
import os

import chimerapy.engine as cpe

CWD = pathlib.Path(os.path.abspath(__file__)).parent


class Producer(cpe.Node):
    def setup(self):
        self.counter = 0

    def step(self):
        time.sleep(1)
        self.logger.debug("Producer step")
        current_counter = self.counter
        self.counter += 1
        data_chunk = cpe.DataChunk()
        data_chunk.add("counter", current_counter)
        return data_chunk


class Consumer(cpe.Node):
    def step(self, data_chunks: Dict[str, cpe.DataChunk]):
        d = data_chunks["prod"].get("counter")["value"]
        self.logger.debug(f"Consumer step: {d}")


class SimpleGraph(cpe.Graph):
    def __init__(self):
        super().__init__()
        self.prod = Producer(name="prod")
        self.cons = Consumer(name="cons")

        self.add_nodes_from([self.prod, self.cons])
        self.add_edge(src=self.prod, dst=self.cons)
        self.node_ids = [self.prod.id, self.cons.id]


if __name__ == "__main__":

    # Create default manager and desired graph
    manager = cpe.Manager(logdir=CWD / "runs")
    manager.zeroconf()
    worker = cpe.Worker(name="local", id="local")

    # Then register graph to Manager
    worker.connect(host=manager.host, port=manager.port)

    # Wait until workers connect
    while True:
        q = input("All workers connected? (Y/n)")
        if q.lower() == "y":
            break

    # For local only
    if len(manager.workers) == 1:
        graph = SimpleGraph()
        mapping = {worker.id: graph.node_ids}
    else:

        # For mutliple workers (remote and local)
        graph = cpe.Graph()
        con_node = Consumer(name="cons")
        graph.add_nodes_from([con_node])
        mapping = {worker.id: [con_node.id]}

        for worker_id in manager.workers:
            if worker_id == "local":
                continue
            else:
                prod_node = Producer(name="prod")
                graph.add_nodes_from([prod_node])
                graph.add_edge(src=prod_node, dst=con_node)
                mapping[worker_id] = [prod_node.id]

    # Commit the graph
    manager.commit_graph(graph=graph, mapping=mapping).result(timeout=60)

    # Wail until user stops
    while True:
        q = input("Ready to start? (Y/n)")
        if q.lower() == "y":
            break

    manager.start().result(timeout=5)
    manager.record().result(timeout=5)

    # Wail until user stops
    while True:
        q = input("Stop? (Y/n)")
        if q.lower() == "y":
            break

    manager.stop().result(timeout=5)
    manager.shutdown()
