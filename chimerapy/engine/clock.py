from datetime import datetime, timedelta
import ntplib
import time

from chimerapy.engine import config

# Globals
time_delta = timedelta(0)


def sync():
    global time_delta
    ntp_client = ntplib.NTPClient()
    for i in range(config.get("sync.attempts")):
        try:
            response = ntp_client.request("pool.ntp.org")
            time_delta = timedelta(seconds=response.offset)
        except:
            time.sleep(0.5)


def utcnow() -> datetime:
    return datetime.utcnow() + time_delta


def now() -> datetime:
    return utcnow()
