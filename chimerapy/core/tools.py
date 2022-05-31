# Built-in Imports
from typing import List, Any
import math
from multiprocessing.queues import Queue
import multiprocessing as mp
import threading
import collections
import queue
import pickle
import open3d as o3d

# Third-Party Imports
from PIL import Image
import numpy as np
import pandas as pd
import psutil

# Helper Classes
Window = collections.namedtuple("Window", ['start', 'end'])

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

class PortableQueue(Queue):
    """ A portable implementation of multiprocessing.Queue.
    Because of multithreading / multiprocessing semantics, Queue.qsize() may
    raise the NotImplementedError exception on Unix platforms like Mac OS X
    where sem_getvalue() is not implemented. This subclass addresses this
    problem by using a synchronized shared counter (initialized to zero) and
    increasing / decreasing its value every time the put() and get() methods
    are called, respectively. This not only prevents NotImplementedError from
    being raised, but also allows us to implement a reliable version of both
    qsize() and empty().

    Code acquired from: 
    https://github.com/vterron/lemon/blob/d60576bec2ad5d1d5043bcb3111dff1fcb58a8d6/methods.py#L536-L573

    According to the StackOver post here:
    https://stackoverflow.com/questions/65609529/python-multiprocessing-queue-notimplementederror-macos
    
    Fixing the `size` not an attribute of Queue can be found here:
    https://stackoverflow.com/questions/69897765/cannot-access-property-of-subclass-of-multiprocessing-queues-queue-in-multiproce
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs, ctx=mp.get_context())
        self.size = SharedCounter(0)
    
    def __getstate__(self):
        return super().__getstate__() + (self.size,)

    def __setstate__(self, state):
        super().__setstate__(state[:-1])
        self.size = state[-1]

    def put(self, *args, **kwargs):

        # Have to account for timeout to make this implementation
        # faithful to the complete mp.Queue implementation.
        try:
            super().put(*args, **kwargs)
            self.size.increment(1)
        except queue.Full:
            raise queue.Full

    def get(self, *args, **kwargs):
        
        # Have to account for timeout to make this implementation
        # faithful to the complete mp.Queue implementation.
        try:
            data = super().get(*args, **kwargs)
            self.size.increment(-1)
            return data
        except queue.Empty:
            raise queue.Empty

    def qsize(self):
        """ Reliable implementation of multiprocessing.Queue.qsize() """
        return self.size.value

    def empty(self):
        """ Reliable implementation of multiprocessing.Queue.empty() """
        return not self.qsize()

    def clear(self):
        """ Remove all elements from the Queue. """
        while not self.empty():
            self.get()

class PointCloudTransmissionFormat:

    def __init__(self, pointcloud: o3d.geometry.PointCloud):
        self.points = np.array(pointcloud.points)
        self.colors = np.array(pointcloud.colors)
        self.normals = np.array(pointcloud.normals)

    def create_pointcloud(self) -> o3d.geometry.PointCloud:
        pointcloud = o3d.geometry.PointCloud()
        pointcloud.points = o3d.utility.Vector3dVector(self.points)
        pointcloud.colors = o3d.utility.Vector3dVector(self.colors)
        pointcloud.normals = o3d.utility.Vector3dVector(self.normals)
        return pointcloud

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
        # print(input_queue.qsize())
        # Make sure to account for possible atomic modification of the
        # queue
        try:
            data = input_queue.get(timeout=0.1)
            del data
        except queue.Empty:
            return
        except EOFError:
            print("Queue EOFError --- data corruption")
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

def get_memory_data_size(data:Any) -> int:
    """Calculate the memory usage of a Python object.

    This was a solution to a memory leak issue. Here is the SO link:
    https://stackoverflow.com/q/71447286/13231446

    The main issue is that multiprocessing.Queue pickles an input and 
    unpickles when using ``get``. Numpy arrays do not handle this well,
    for their memory meta data is corrupted when this happends. This 
    caused memory to not be accurately computed. The solution was to 
    repickle the data and measure the len of the pickle string. This is
    a temporary solution, as I would like NumPy to solve this issue.

    Args:
        data (Any): The python object in question.

    Returns:
        int: Size of the Python object in bytes.
    """
    return len(pickle.dumps(data))

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
