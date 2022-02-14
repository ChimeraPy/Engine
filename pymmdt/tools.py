# Built-in Imports
from typing import List
import math
import multiprocessing as mp
import threading
import collections
import queue

# Third-Party Imports
from PIL import Image
import numpy as np
import pandas as pd

# Helper Classes
Window = collections.namedtuple("Window", ['start', 'end'])

# From: https://stackoverflow.com/a/19846691/13231446 
def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        # thread.start()
        thread.deamon = True
        return thread
    return wrapper

def multiprocessed(fn):
    def wrapper(*args, **kwargs):
        process = multiprocessing.Process(target=fn, args=args, kwargs=kwargs)
        # thread.start()
        process.deamon = True
        return process
    return wrapper

def clear_queue(input_queue: mp.Queue):
    # print(input_queue.qsize())

    while input_queue.qsize() != 0:
        # print(input_queue.qsize())

        # Make sure to account for possible automic modification of the
        # queue
        try:
            data = input_queue.get(timeout=0.1)
            del data
        except queue.Empty:
            continue

def get_windows(
        start_time:pd.Timedelta, 
        end_time:pd.Timedelta, 
        time_window_size:pd.Timedelta
    ) -> List[Window]:

    # Determine how many time windows given the total time and size
    total_time = (end_time - start_time)
    num_of_windows = math.ceil(total_time / time_window_size)

    # Create unique namedtuple and storage for the Windows
    windows = []

    # For all the possible windows, calculate their start and end
    for x in range(num_of_windows):
        start = start_time + x * time_window_size 
        end = start_time + (x+1) * time_window_size
        capped_end = min(end_time+pd.Timedelta(seconds=0.1), end)
        window = Window(start, capped_end)
        windows.append(window)

    return windows

# Got this function from: https://uploadcare.com/blog/fast-import-of-pillow-images-to-numpy-opencv-arrays/
def to_numpy(im:Image):

    # Load the image
    im.load()

    # unpack data
    e = Image._getencoder(im.mode, 'raw', im.mode)
    e.setimage(im.im)

    # NumPy buffer for the result
    shape, typestr = Image._conv_type_shape(im)
    data = np.empty(shape, dtype=np.dtype(typestr))
    mem = data.data.cast('B', (data.data.nbytes,))

    bufsize, s, offset = 65536, 0, 0
    while not s:
        l, s, d = e.encode(bufsize)
        mem[offset:offset + len(d)] = d
        offset += len(d)
    if s < 0:
        raise RuntimeError("encoder error %d in tobytes" % s)

    return data
