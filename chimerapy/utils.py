from typing import Callable, Union, Optional
import queue
import logging
import functools
import time

from tqdm import tqdm

from . import _logger

logger = _logger.getLogger("chimerapy")


def clear_queue(input_queue: queue.Queue):
    """Clear a queue.
    Args:
        input_queue (queue.Queue): Queue to be cleared.
    """

    while input_queue.qsize() != 0:
        logger.debug(f"size={input_queue.qsize()}")
        # Make sure to account for possible atomic modification of the
        # queue
        try:
            data = input_queue.get(timeout=0.1, block=False)
            del data
        except queue.Empty:
            logger.debug(f"clear_queue: empty!")
            return
        except EOFError:
            logger.warning("Queue EOFError --- data corruption")
            return


# References:
# https://github.com/tqdm/tqdm/issues/313#issuecomment-850698822


class logging_tqdm(tqdm):
    def __init__(
        self,
        *args,
        logger: logging.Logger = None,
        mininterval: float = 1,
        bar_format: str = "{desc}{percentage:3.0f}%{r_bar}",
        desc: str = "progress: ",
        **kwargs,
    ):
        self._logger = logger
        super().__init__(
            *args, mininterval=mininterval, bar_format=bar_format, desc=desc, **kwargs
        )

    @property
    def logger(self):
        if self._logger is not None:
            return self._logger
        return logger

    def display(self, msg=None, pos=None):
        if not self.n:
            # skip progress bar before having processed anything
            return
        if not msg:
            msg = self.__str__()
        self.logger.info("%s", msg)


def waiting_for(
    condition: Callable[[], bool],
    check_period: Union[int, float] = 0.1,
    timeout: Optional[Union[int, float]] = None,
    timeout_msg: Optional[str] = None,
) -> bool:

    counter = 0
    while True:

        if condition():
            return True
        else:
            time.sleep(check_period)
            counter += 1

            if timeout and counter * check_period > timeout:
                raise TimeoutError(timeout_msg)
