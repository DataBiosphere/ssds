from typing import (
    Optional,
)
from concurrent.futures import ThreadPoolExecutor

MiB = 1024 ** 2

AWS_MIN_CHUNK_SIZE = 64 * MiB
"""Files must be larger than this before we consider multipart uploads."""

MULTIPART_THRESHOLD = AWS_MIN_CHUNK_SIZE + 1
"""Convenience variable for Boto3 TransferConfig(multipart_threhold=)."""

AWS_MAX_MULTIPART_COUNT = 10000
"""Maximum number of parts allowed in a multipart upload.  This is a limitation imposed by S3."""


class BlobStore:
    schema: Optional[str] = None

    def upload_object(self, filepath: str, bucket: str, key: str):
        raise NotImplementedError()

    def list(self, bucket_name: str, prefix=""):
        raise NotImplementedError()

    def get(self, bucket_name: str, key: str) -> bytes:
        raise NotImplementedError()

    def put(self, bucket_name: str, key: str, data: bytes):
        raise NotImplementedError()

    def cloud_native_checksum(self, bucket_name: str, key: str) -> str:
        raise NotImplementedError()

class AsyncPartIterator:
    def __init__(self, bucket_name, key, executor: ThreadPoolExecutor=None):
        self.size = -1
        self.chunk_size = -1
        self.number_of_parts = -1

    def __iter__(self):
        raise NotImplementedError()
