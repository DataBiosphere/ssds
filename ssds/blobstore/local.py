import os
from math import ceil
from typing import Generator, Optional

from ssds.blobstore import (BlobStore, Blob, AsyncPartIterator, Part, MultipartWriter, get_s3_multipart_chunk_size,
                            BlobNotFoundError, BlobStoreUnknownError)


class LocalBlobStore(BlobStore):
    def list(self, prefix: str="") -> Generator[str, None, None]:
        for (dirpath, dirnames, filenames) in os.walk(prefix):
            for filename in filenames:
                relpath = os.path.join(dirpath, filename)
                yield os.path.abspath(relpath)

    def blob(self, key: str) -> "LocalBlob":
        return LocalBlob(key)

class LocalBlob(Blob):
    def __init__(self, path: str):
        self.key = path

    def size(self) -> int:
        return os.path.getsize(self.key)

    def parts(self):
        return LocalAsyncPartIterator(self.key)

class LocalAsyncPartIterator:
    def __init__(self, key: str):
        self.size = os.path.getsize(key)
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
