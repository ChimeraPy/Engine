# Built-in Imports
from typing import Dict, List
import time
import pathlib
import os
import multiprocessing as mp
from collections import Counter

# ChimeraPy Library
from chimerapy.core.data_chunk import DataChunk
from chimerapy.core.process import Process
from chimerapy.core.graph.graph import Graph
# from chimerapy.core.tabular import TabularDataStream
# from chimerapy.core.video import VideoDataStream
# from chimerapy.core.reader import Reader
# from chimerapy.core.writer import Writer

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

# Logging
import logging
logger = logging.getLogger(__name__)


class SimpleProducer(Process):
    def __init__(self, name: str, verbose: bool = False):
        self.counter = mp.Value("i", 0)
        super().__init__(name, verbose)

    def step(self, data_chunks: List[DataChunk]):
        with self.counter.get_lock():
            self.counter.value += 1
        
        if self.counter.value == 10:
            return "END"

        return self.counter.value

class Reducer(Process):
    def step(self, data_chunks: List[DataChunk]):
        result = 0
        # logger.debug(f"from reducer {self.name}, <- {[(chunk.owner, chunk.data) for chunk in data_chunks]}")
        for chunk in data_chunks:
            if chunk.owner == "process1":
                result = result + chunk.data
            elif chunk.owner == "process2":
                result = result + chunk.data * 2
            elif chunk.owner == "process3":
                result = result + chunk.data * 3

        logger.debug(f"from reducer {self.name}, {result} ")
        return result

class SimpleWriter(Process):
    def setup(self):
        logger.debug("Running writer setup")
        self.out_file = open("test.txt", "a")

    def step(self, data_chunks: List[DataChunk]):
        value = sum(chunk.data for chunk in data_chunks)
        logger.debug(f"from writer {value} <- {[(chunk.owner, chunk.data) for chunk in data_chunks]}")
        self.out_file.write(f"{value} ")

    def wrapup(self):
        logger.debug("wrapup called")
        self.out_file.close()



def test_graph_run():
    graph = Graph("test_graph", 5)
    process1 = SimpleProducer("process1")
    process2 = SimpleProducer("process2")
    process3 = SimpleProducer("process3")

    reducer1 = Reducer("reducer1")
    reducer2 = Reducer("reducer2")
    writer = SimpleWriter("writer")

    graph.add_node(process1)
    graph.add_node(process2)
    graph.add_node(process3)
    graph.add_node(reducer1)
    graph.add_node(reducer2)
    graph.add_node(writer)

    graph.add_edge(process1, reducer1, "process1->reducer1")
    graph.add_edge(process2, reducer1, "process2->reducer1")
    graph.add_edge(process2, reducer2, "process2->reducer2")
    graph.add_edge(process3, reducer2, "process3->reducer2")

    graph.add_edge(reducer1, writer, "reducer1->writer")
    graph.add_edge(reducer2, writer, "reducer2->writer")

    assert Counter(process1.in_queues) == Counter(process2.in_queues)
    assert Counter(process2.in_queues) == Counter(process3.in_queues)

    assert (process1.out_queues[0] in reducer1.in_queues) == True
    assert (process2.out_queues[0] in reducer1.in_queues) == True
    assert (process2.out_queues[1] in reducer2.in_queues) == True
    assert (process3.out_queues[0] in reducer2.in_queues) == True

    assert (reducer1.out_queues[0] in writer.in_queues) == True
    assert (reducer2.out_queues[0] in writer.in_queues) == True

    graph.start()
    time.sleep(5)
    graph.shutdown()

    with open("test.txt", "r") as in_file:
        assert in_file.read() == "8 16 24 32 40 48 56 64 72 "


"""
0
 \
  \
  0
 /  \ 
/    \
0    0 -> writes to a file
 \  /
  \/
  0
 /  
/
0
"""