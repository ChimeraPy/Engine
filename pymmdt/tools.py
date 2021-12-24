import multiprocessing
import threading

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

