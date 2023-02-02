from typing import Callable, Union, Optional, Any, Dict
import queue
import logging
import functools
import time
import enum
import pickle
import datetime
import uuid
import socket
import asyncio

# Third-party
from tqdm import tqdm
import blosc
import netifaces as ni

# Internal
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


async def async_waiting_for(
    condition: Callable[[], bool],
    check_period: Union[int, float] = 0.1,
    timeout: Optional[Union[int, float]] = None,
    timeout_raise: Optional[bool] = False,
) -> bool:

    counter = 0
    while True:

        if condition():
            return True
        else:
            await asyncio.sleep(check_period)
            counter += 1

            if timeout and counter * check_period > timeout:
                if timeout_raise:
                    raise TimeoutError(str(condition) + ": FAILURE")
                else:
                    return False


def waiting_for(
    condition: Callable[[], bool],
    check_period: Union[int, float] = 0.1,
    timeout: Optional[Union[int, float]] = None,
    timeout_raise: Optional[bool] = False,
) -> bool:

    counter = 0
    while True:

        if condition():
            return True
        else:
            time.sleep(check_period)
            counter += 1

            if timeout and counter * check_period > timeout:
                if timeout_raise:
                    raise TimeoutError(str(condition) + ": FAILURE")
                else:
                    return False


def get_open_port(start_port: int) -> socket.socket:

    # Creating socket to connect
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    offset = 0

    while True:
        try:
            current_attempt_port = start_port + offset
            s.bind(("", current_attempt_port))
            break
        except socket.error as e:
            offset += 10
            if e.errno == errno.EADDRINUSE:
                logger.debug(f"Port {current_attempt_port} is already in use.")
            else:
                logger.error("Unknown socket error", exc_info=True)
                raise e

    return s


def get_ip_address() -> str:

    # Get gateway of the network
    gws = ni.gateways()
    default_gw_name = gws["default"][ni.AF_INET][1]

    # Get the ip in the default gateway
    ip = ni.ifaddresses(default_gw_name)[ni.AF_INET][0]["addr"]
    return ip


def create_payload(
    signal: enum.Enum,
    data: Any,
    msg_uuid: str = str(uuid.uuid4()),
    timestamp: datetime.timedelta = datetime.timedelta(),
    ok: bool = False,
) -> bytes:

    payload = {
        "signal": signal,
        "timestamp": str(timestamp),
        "data": data,
        "uuid": msg_uuid,
        "ok": ok,
    }

    return blosc.compress(pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL))


def decode_payload(data: bytes) -> Dict[str, Any]:
    return pickle.loads(blosc.decompress(data))
