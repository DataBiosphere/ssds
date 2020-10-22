import time
from functools import wraps
from datetime import datetime


_datetime_format = "%Y-%m-%dT%H%M%S.%fZ"

def timestamp(dt: datetime) -> str:
    return dt.strftime(_datetime_format)

def timestamp_now() -> str:
    return timestamp(datetime.utcnow())

def datetime_from_timestamp(ts: str) -> datetime:
    return datetime.strptime(ts, _datetime_format)

def retry(*exceptions, number_of_attempts=5, initial_wait=0.2, backoff_factor=2):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            wait = initial_wait
            for tries_remaining in range(number_of_attempts - 1, -1, -1):
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    if tries_remaining:
                        time.sleep(wait)
                        wait *= backoff_factor
                    else:
                        raise
        return wrapper
    return decorator
