from typing import Any, Dict, Tuple
import socket
import errno
import datetime
import struct
import pickle

# Third-party imports
import netifaces as ni
from tqdm import tqdm
import blosc

from .. import _logger

logger = _logger.getLogger("chimerapy")


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
    signal: int,
    data: Any,
    timestamp: datetime.timedelta = datetime.timedelta(),
) -> bytes:

    payload = {
        "signal": signal,
        "timestamp": str(timestamp),
        "data": data,
    }

    return blosc.compress(pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL))


def decode_payload(data: bytes) -> Dict[str, Any]:
    return pickle.loads(blosc.decompress(data))
