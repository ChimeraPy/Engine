from datetime import datetime, timedelta

time_delta = timedelta(0)


def update_reference_time(delta: timedelta):
    global time_delta
    time_delta = delta


def utcnow() -> datetime:
    return datetime.utcnow() - time_delta


def now() -> datetime:
    return utcnow()
