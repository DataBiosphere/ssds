from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from gs_chunked_io.async_collections import AsyncSet, AsyncQueue


class Executor:
    max_workers = 3
    _executor: Optional[ThreadPoolExecutor] = None

    @classmethod
    def get(cls) -> ThreadPoolExecutor:
        cls._executor = cls._executor or ThreadPoolExecutor(max_workers=cls.max_workers)
        return cls._executor

    @classmethod
    def shutdown(cls):
        if cls._executor:
            cls._executor.shutdown(wait=True)
            cls._executor = None

def async_set():
    return AsyncSet(Executor.get(), Executor.max_workers)

def async_queue():
    return AsyncQueue(Executor.get(), Executor.max_workers)
