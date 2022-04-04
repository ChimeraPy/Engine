# Built-in Imports
import unittest
import pathlib
import shutil
import os
import sys
import time
import collections
import queue
import gc
import multiprocessing as mp

# Third-Party Imports
import tqdm
import numpy as np
import psutil
import pandas as pd

# Testing Library
import chimerapy as cp

# Constants
CURRENT_DIR = pathlib.Path(os.path.dirname(os.path.abspath(__file__)))
TEST_DIR = CURRENT_DIR.parent
RAW_DATA_DIR = TEST_DIR / 'data' 
OUTPUT_DIR = TEST_DIR / 'test_output' 

def clear_queue(in_queue, out_queue):
    print("clear_queue running! - ", in_queue.qsize(), out_queue.qsize())

    while in_queue.qsize() != 0:
        print(".", end='')
        out_queue.put(in_queue.get())

    print("clear_queue finished!")

class PortableQueueTests(unittest.TestCase):

    def test_putting_and_getting(self):

        q_max_size = 10
        queue = cp.tools.PortableQueue(maxsize=q_max_size)

        for i in range(q_max_size-1):
            queue.put('a')

        while queue.qsize() != 0:
            queue.get()

    def test_passing_queue_to_process(self):

        number_of_entries = 5
        in_queue = cp.tools.PortableQueue(maxsize=10)
        out_queue = cp.tools.PortableQueue(maxsize=10)

        for i in range(number_of_entries):
            in_queue.put(i)

        print(in_queue.qsize(), out_queue.qsize())
        process = mp.Process(target=clear_queue, args=(in_queue, out_queue,))
        process.start()
        print("process started!")

        while in_queue.qsize() != 0:
            time.sleep(0.1)

        counter = 0
        while out_queue.qsize() != 0:
            out_queue.get()
            counter += 1

        process.join()

        assert counter == number_of_entries

if __name__ == '__main__':
    unittest.main()
