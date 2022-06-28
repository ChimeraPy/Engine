# Built-in Imports
from typing import List, Any
import math
from multiprocessing.queues import Queue
import multiprocessing as mp
import threading
import collections
import queue
import pickle
# import open3d as o3d
import logging

# Third-Party Imports
from PIL import Image
import numpy as np
import pandas as pd
import psutil
import types

# Helper Classes
Window = collections.namedtuple("Window", ['start', 'end'])

# Logging
logger = logging.getLogger(__name__)

class SharedCounter(object):
    """ A synchronized shared counter.
    The locking done by multiprocessing.Value ensures that only a single
    process or thread may read or write the in-memory ctypes object. However,
    in order to do n += 1, Python performs a read followed by a write, so a
    second process may read the old value before the new one is written by the
    first process. The solution is to use a multiprocessing.Lock to guarantee
    the atomicity of the modifications to Value.
    This class comes almost entirely from Eli Bendersky's blog:
    http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing/
    """

    def __init__(self, n = 0):
        self.count = mp.Value('i', n)

    def increment(self, n = 1):
        """ Increment the counter by n (default = 1) """
        with self.count.get_lock():
            self.count.value += n

    @property
    def value(self):
        """ Return the value of the counter """
        return self.count.value

def threaded(fn):
    """Decorator for class methods to be spawn new thread.
    
    From: https://stackoverflow.com/a/19846691/13231446 

    Args:
        fn: The method of a class.
    """
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        # thread.start()
        thread.deamon = True
        return thread
    return wrapper

def clear_queue(input_queue: Queue):
    """Clear a queue.

    Args:
        input_queue (mp.Queue): Queue to be cleared.
    """

    while input_queue.qsize() != 0:
        logger.debug(f'tools.clear_queue, size={input_queue.qsize()}')
        # Make sure to account for possible atomic modification of the
        # queue
        try:
            data = input_queue.get(timeout=0.1, block=False)
            del data
        except queue.Empty:
            logger.debug(f'tools.clear_queue, empty!')
            return
        except EOFError:
            logger.warning("Queue EOFError --- data corruption")
            return 

def get_windows(
        start_time:pd.Timedelta, 
        end_time:pd.Timedelta, 
        time_window_size:pd.Timedelta
    ) -> List[Window]:
    """Compute the start and end times of the windows found in range.

    Args:
        start_time (pd.Timedelta): The start time of the range.
        end_time (pd.Timedelta): The end time of the range.
        time_window_size (pd.Timedelta): The size of the time window.

    Returns:
        List[Window]: A list of the window (namedtuples with ``start``
        and ``end`` attributes).
    """

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

def to_numpy(im:Image) -> np.ndarray:
    """Convert a PIL Image to numpy np.ndarray.
    
    Got this function from: 
    https://uploadcare.com/blog/fast-import-of-pillow-images-to-numpy-opencv-arrays/

    Args:
        im (Image): The PIL image.

    Returns:
        np.ndarray: The numpy image.
    """

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

def get_threads_cpu_percent(p:psutil.Process, interval:float=0.1) -> List:
    # Got this from:
    # https://stackoverflow.com/a/46401536/13231446

    # First, check if the process is still running 
    if not p.is_running():
        return []
    
    total_percent = p.cpu_percent(interval)
    total_time = sum(p.cpu_times())
    
    # Account for a division by zero
    if total_time == 0:
        return []

    return [total_percent * ((t.system_time + t.user_time)/total_time) for t in p.threads()]


def Proxy(target):
    """
        Create a derived NamespaceProxy class for `target`.
        Exposes every attribute of the target in the proxy
    """
    def __getattr__(self, key):
        result = self._callmethod('__getattribute__', (key,))
        if isinstance(result, types.MethodType):
            def wrapper(*args, **kwargs):
                return self._callmethod(key, args, kwargs)
            return wrapper
        return result

    dic = {'types': types, '__getattr__': __getattr__}
    proxy_name = target.__name__ + "Proxy"
    ProxyType = type(proxy_name, (mp.managers.NamespaceProxy,), dic)  # Create subclass.
    ProxyType._exposed_ = tuple(dir(target))

    return ProxyType
