import queue
import logging
import json
import time
import enum
import datetime
import uuid
import socket
import asyncio
import errno
from typing import Callable, Union, Optional, Any, Dict

# Third-party

# Internal
from chimerapy.engine import _logger

logger = _logger.getLogger("chimerapy-engine")

BYTES_PER_MB = 1024 * 1024


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
            input_queue.get(timeout=0.1, block=False)
        except queue.Empty:
            logger.debug("clear_queue: empty!")
            return
        except EOFError:
            logger.warning("Queue EOFError --- data corruption")
            return


# References:
# https://github.com/tqdm/tqdm/issues/313#issuecomment-850698822


class TqdmToLogger(object):
    """Adapter to redirect tqdm output to a logger"""

    def __init__(self, logger, level=logging.INFO):
        self.logger = logger
        self.level = level
        self.buf = ""

    def write(self, buf):
        if buf.rstrip():
            self.logger.log(self.level, buf.rstrip())

    def flush(self):
        pass


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
    """Get the IP address of the current machine."""

    # https://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)

    try:
        # doesn't even have to be reachable
        s.connect(("10.254.254.254", 1))
        ip = str(s.getsockname()[0])
    except:  # noqa E722
        ip = "127.0.0.1"
    finally:
        s.close()

    return ip


def create_payload(
    signal: enum.Enum,
    data: Any,
    msg_uuid: str = str(uuid.uuid4()),
    timestamp: datetime.timedelta = datetime.timedelta(),
    ok: bool = False,
) -> Dict[str, Any]:

    payload = {
        "signal": signal.value,
        "timestamp": str(timestamp),
        "data": data,
        "uuid": msg_uuid,
        "ok": ok,
    }

    return payload


def decode_payload(data: str) -> Dict[str, Any]:
    return json.loads(data)


def megabytes_to_bytes(megabytes: int) -> int:
    return int(megabytes) * BYTES_PER_MB


def update_dataclass(target, source):

    for field in target.__dataclass_fields__.keys():
        # Check if the source field has a non-None value before updating
        if getattr(source, field) is not None:
            setattr(target, field, getattr(source, field))
