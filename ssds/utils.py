import time
from functools import wraps


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
