import os
from math import ceil
from functools import wraps
from typing import Generator, Optional

from ssds.blobstore import (BlobStore, Blob, AsyncPartIterator, Part, MultipartWriter, get_s3_multipart_chunk_size,
                            BlobNotFoundError, BlobStoreUnknownError)


def catch_blob_not_found(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except FileNotFoundError as ex:
            raise BlobNotFoundError(f"Could not find {self.key}") from ex
    return wrapper

class LocalBlobStore(BlobStore):
    def __init__(self, base_path: str):
        self.base_path = base_path

    def list(self, prefix: str="") -> Generator[str, None, None]:
        for (dirpath, dirnames, filenames) in os.walk(os.path.join(self.base_path, prefix)):
            for filename in filenames:
                relpath = os.path.join(dirpath, filename)
                yield os.path.abspath(relpath)

    def blob(self, key: str) -> "LocalBlob":
        return LocalBlob(key)

class LocalBlob(Blob):
    def __init__(self, path: str):
        self.key = path

    @catch_blob_not_found
    def get(self) -> bytes:
        with open(self.key, "rb") as fh:
            return fh.read()

    @catch_blob_not_found
    def size(self) -> int:
        return os.path.getsize(self.key)

    def parts(self):
        return LocalAsyncPartIterator(self.key)

class LocalAsyncPartIterator:
    def __init__(self, key: str):
        try:
            self.size = os.path.getsize(key)
        except FileNotFoundError:
            raise BlobNotFoundError(f"Could not find {key}")
        self.chunk_size = get_s3_multipart_chunk_size(self.size)
        self._number_of_parts = ceil(self.size / self.chunk_size) if 0 < self.size else 1
        self.handle = open(key, "rb")

    def __len__(self):
        return self._number_of_parts

    def __iter__(self) -> Generator[Part, None, None]:
        for part_number in range(self._number_of_parts):
            yield self._get_part(part_number)

    def _get_part(self, part_number: int) -> Part:
        self.handle.seek(part_number * self.chunk_size)
        return Part(part_number, self.handle.read(self.chunk_size))

    def close(self):
        self.handle.close()

    def __del__(self):
        self.close()
