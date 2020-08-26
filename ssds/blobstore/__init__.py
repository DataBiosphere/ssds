from math import ceil
from collections import namedtuple
from typing import Dict, Optional, Generator


MiB = 1024 ** 2

AWS_MIN_CHUNK_SIZE = 64 * MiB
"""Files must be larger than this before we consider multipart uploads."""

MULTIPART_THRESHOLD = AWS_MIN_CHUNK_SIZE + 1
"""Convenience variable for Boto3 TransferConfig(multipart_threhold=)."""

AWS_MAX_MULTIPART_COUNT = 10000
"""Maximum number of parts allowed in a multipart upload.  This is a limitation imposed by S3."""

class BlobStore:
    def list(self, prefix: str=""):
        raise NotImplementedError()

    def blob(self, key: str) -> "Blob":
        raise NotImplementedError()

class Blob:
    schema = ""

    def put_tags(self, tags: Dict[str, str]):
        raise NotImplementedError()

    def get_tags(self) -> Dict[str, str]:
        raise NotImplementedError()

    def get(self) -> bytes:
        raise NotImplementedError()

    def put(self, data: bytes):
        raise NotImplementedError()

    def exists(self) -> bool:
        raise NotImplementedError()

    def size(self) -> int:
        raise NotImplementedError()

    def cloud_native_checksum(self) -> str:
        raise NotImplementedError()

    def parts(self, threads: Optional[int]=None) -> "AsyncPartIterator":
        raise NotImplementedError()

    def multipart_writer(self, threads: Optional[int]=None) -> "MultipartWriter":
        raise NotImplementedError()

Part = namedtuple("Part", "number data")

class AsyncPartIterator:
    def __init__(self, bucket_name, key, threads: Optional[int]=None):
        self.size = 0
        self.chunk_size = 0
        self._number_of_parts = 0

    def __len__(self):
        return self._number_of_parts

    def __iter__(self) -> Generator[Part, None, None]:
        raise NotImplementedError()

class MultipartWriter:
    def put_part(self, part: Part):
        raise NotImplementedError()

    def close(self):
        raise NotImplementedError()

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

def get_s3_multipart_chunk_size(filesize: int) -> int:
    """Returns the chunk size of the S3 multipart object, given a file's size."""
    if filesize <= AWS_MAX_MULTIPART_COUNT * AWS_MIN_CHUNK_SIZE:
        return AWS_MIN_CHUNK_SIZE
    else:
        raw_part_size = ceil(filesize / AWS_MAX_MULTIPART_COUNT)
        part_size_in_integer_megabytes = ((raw_part_size + MiB - 1) // MiB) * MiB
        return part_size_in_integer_megabytes
