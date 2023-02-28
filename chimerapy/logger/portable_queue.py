import multiprocessing as mp
import queue


class SharedCounter:
    """A synchronized shared counter.
    The locking done by multiprocessing.Value ensures that only a single
    process or thread may read or write the in-memory ctypes object. However,
    in order to do n += 1, Python performs a read followed by a write, so a
    second process may read the old value before the new one is written by the
    first process. The solution is to use a multiprocessing.Lock to guarantee
    the atomicity of the modifications to Value.
    This class comes almost entirely from Eli Bendersky's blog:
    http://eli.thegreenplace.net/2012/01/04/shared-counter-with-pythons-multiprocessing/
    """

    def __init__(self, n=0):
        self.count = mp.Value("i", n)

    def increment(self, n=1):
        """Increment the counter by n (default = 1)"""
        with self.count.get_lock():
            self.count.value += n

    @property
    def value(self):
        """Return the value of the counter"""
        return self.count.value


class PortableQueue(mp.Queue):
    """A portable implementation of multiprocessing.Queue.
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
        """Reliable implementation of multiprocessing.Queue.qsize()"""
        return self.size.value

    def empty(self):
        """Reliable implementation of multiprocessing.Queue.empty()"""
        return not self.qsize()

    def clear(self):
        """Remove all elements from the Queue."""
        while not self.empty():
            self.get()
