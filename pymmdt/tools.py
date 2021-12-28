# Built-in Imports
import math
import multiprocessing
import threading
import collections

# Third-Party Imports
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

def get_windows(start_time, end_time, time_window_size):

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
