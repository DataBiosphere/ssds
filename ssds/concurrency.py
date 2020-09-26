from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from gs_chunked_io.async_collections import AsyncSet, AsyncQueue


MAX_RPC_CONCURRENCY = 30         # Copy operations without passing data through local machine
MAX_PASSTHROUGH_CONCURRENCY = 4  # Copy operatires requiring passthrough

class Executor:
    max_workers = MAX_RPC_CONCURRENCY + MAX_PASSTHROUGH_CONCURRENCY
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

def async_set(concurrency: int=MAX_PASSTHROUGH_CONCURRENCY):
    return AsyncSet(Executor.get(), concurrency)

def async_queue(concurrency: int=MAX_PASSTHROUGH_CONCURRENCY):
    return AsyncQueue(Executor.get(), concurrency)
