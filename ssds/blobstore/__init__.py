import os
from math import ceil
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Tuple, Optional, Generator

from ssds import checksum


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
        size = os.stat(filepath).st_size
        chunk_size = get_s3_multipart_chunk_size(size)
        if chunk_size >= size:
            s3_etag, gs_crc32c = self._upload_oneshot(filepath, bucket, key)
        else:
            s3_etag, gs_crc32c = self._upload_multipart(filepath, bucket, key, chunk_size)
        if "s3://" == self.schema:
            assert s3_etag == self.cloud_native_checksum(bucket, key)
        elif "gs://" == self.schema:
            assert gs_crc32c == self.cloud_native_checksum(bucket, key)
        else:
            raise ValueError(f"Unsuported schema: {self.schema}")
        tags = {SSDSObjectTag.SSDS_MD5: s3_etag, SSDSObjectTag.SSDS_CRC32C: gs_crc32c}
        self.put_tags(bucket, key, tags)

    def _upload_oneshot(self, filepath: str, bucket: str, key: str) -> Tuple[str, str]:
        with open(filepath, "rb") as fh:
            data = fh.read()
        gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
        s3_etag = checksum.md5(data).hexdigest()
        self.put(bucket, key, data)
        return s3_etag, gs_crc32c

    def _upload_multipart(self, filepath: str, bucket_name: str, key: str, part_size: int) -> Tuple[str, str]:
        s3_etags = list()
        crc32c = checksum.crc32c(b"")
        with self.multipart_writer(bucket_name, key) as uploader:
            part_number = 0
            with open(filepath, "rb") as fh:
                while True:
                    data = fh.read(part_size)
                    if not data:
                        break
                    s3_etags.append(checksum.md5(data).hexdigest())
                    crc32c.update(data)
                    uploader.put_part(Part(part_number, data))
                    part_number += 1
        return checksum.compute_composite_etag(s3_etags), crc32c.google_storage_crc32c()

    def put_tags(self, bucket_name: str, key: str, tags: Dict[str, str]):
        raise NotImplementedError()

    def get_tags(self, bucket_name: str, key: str) -> Dict[str, str]:
        raise NotImplementedError()

    def list(self, bucket_name: str, prefix=""):
        raise NotImplementedError()

    def get(self, bucket_name: str, key: str) -> bytes:
        raise NotImplementedError()

    def put(self, bucket_name: str, key: str, data: bytes):
        raise NotImplementedError()

    def cloud_native_checksum(self, bucket_name: str, key: str) -> str:
        raise NotImplementedError()

    def parts(self, bucket_name: str, key: str, executor: ThreadPoolExecutor=None) -> "AsyncPartIterator":
        raise NotImplementedError()

    def multipart_writer(self, bucket_name: str, key: str, executor: ThreadPoolExecutor=None) -> "MultipartWriter":
        raise NotImplementedError()

Part = namedtuple("Part", "number data")

class SSDSObjectTag:
    SSDS_MD5 = "SSDS_MD5"
    SSDS_CRC32C = "SSDS_CRC32C"

class AsyncPartIterator:
    def __init__(self, bucket_name, key, executor: ThreadPoolExecutor=None):
        self.size: Optional[int] = None
        self.chunk_size: Optional[int] = None
        self._number_of_parts: Optional[int] = None

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
