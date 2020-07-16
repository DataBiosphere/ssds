import io
import os
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from math import ceil
from typing import (
    Any,
    Set,
    List,
    Tuple,
    Optional,
    Generator,
)

from ssds import aws, checksum
from ssds.blobstore import MiB, AWS_MIN_CHUNK_SIZE, AWS_MAX_MULTIPART_COUNT, BlobStore, AsyncPartIterator, Part

class S3BlobStore(BlobStore):
    schema = "s3://"

    def upload_object(self, filepath: str, bucket: str, key: str):
        size = os.stat(filepath).st_size
        chunk_size = get_s3_multipart_chunk_size(size)
        if chunk_size >= size:
            s3_etag, gs_crc32c = self._upload_oneshot(filepath, bucket, key)
        else:
            s3_etag, gs_crc32c = _upload_multipart(filepath, bucket, key, chunk_size)
        tags = dict(TagSet=[dict(Key="SSDS_MD5", Value=s3_etag), dict(Key="SSDS_CRC32C", Value=gs_crc32c)])
        aws.client("s3").put_object_tagging(Bucket=bucket, Key=key, Tagging=tags)

    def _upload_oneshot(self, filepath: str, bucket: str, key: str):
        blob = aws.resource("s3").Bucket(bucket).Object(key)
        with open(filepath, "rb") as fh:
            data = fh.read()
        gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
        s3_etag = checksum.md5(data).hexdigest()
        self.put(bucket, key, data)
        assert s3_etag == blob.e_tag.strip("\"")
        return s3_etag, gs_crc32c

    def list(self, bucket: str, prefix=""):
        for item in aws.resource("s3").Bucket(bucket).objects.filter(Prefix=prefix):
            yield item.key

    def get(self, bucket_name: str, key: str) -> bytes:
        with closing(aws.resource("s3").Bucket(bucket_name).Object(key).get()['Body']) as fh:
            return fh.read()

    def put(self, bucket_name: str, key: str, data: bytes):
        blob = aws.resource("s3").Bucket(bucket_name).Object(key)
        blob.upload_fileobj(io.BytesIO(data))

    def cloud_native_checksum(self, bucket_name: str, key: str) -> str:
        blob = aws.resource("s3").Bucket(bucket_name).Object(key)
        return blob.e_tag.strip("\"")

    def parts(self, bucket_name: str, key: str, executor: ThreadPoolExecutor=None) -> "S3AsyncPartIterator":
        return S3AsyncPartIterator(bucket_name, key, executor)

class S3AsyncPartIterator(AsyncPartIterator):
    parts_to_buffer = 2

    def __init__(self, bucket_name, key, executor: ThreadPoolExecutor=None):
        self._blob = aws.resource("s3").Bucket(bucket_name).Object(key)
        self.size = self._blob.content_length
        self.chunk_size = get_s3_multipart_chunk_size(self.size)
        self.number_of_parts = ceil(self.size / self.chunk_size)
        self._executor = executor or ThreadPoolExecutor(max_workers=4)

    def __iter__(self) -> Generator[Part, None, None]:
        if 1 == self.number_of_parts:
            yield self._get_part(0)
        else:
            futures: Set[Future] = set()
            part_numbers = [part_number for part_number in range(self.number_of_parts)]
            while part_numbers or futures:
                if len(futures) < self.parts_to_buffer:
                    number_of_parts_to_fetch = self.parts_to_buffer - len(futures)
                    for part_number in part_numbers[:number_of_parts_to_fetch]:
                        futures.add(self._executor.submit(self._get_part, part_number))
                    part_numbers = part_numbers[number_of_parts_to_fetch:]
                for f in as_completed(futures):
                    part = f.result()
                    futures.remove(f)
                    yield part
                    break  # Break out of inner loop to avoid waiting for `as_completed` to provide next future

    def _get_part(self, part_number: int) -> Part:
        offset = part_number * self.chunk_size
        byte_range = f"bytes={offset}-{offset + self.chunk_size - 1}"
        data = self._blob.get(Range=byte_range)['Body'].read()
        return Part(part_number, data)

def get_s3_multipart_chunk_size(filesize: int):
    """Returns the chunk size of the S3 multipart object, given a file's size."""
    if filesize <= AWS_MAX_MULTIPART_COUNT * AWS_MIN_CHUNK_SIZE:
        return AWS_MIN_CHUNK_SIZE
    else:
        raw_part_size = ceil(filesize / AWS_MAX_MULTIPART_COUNT)
        part_size_in_integer_megabytes = ((raw_part_size + MiB - 1) // MiB) * MiB
        return part_size_in_integer_megabytes

def _upload_oneshot(filepath: str, bucket: str, key: str):
    blob = aws.resource("s3").Bucket(bucket).Object(key)
    with open(filepath, "rb") as fh:
        data = fh.read()
    gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
    s3_etag = checksum.md5(data).hexdigest()
    blob.upload_fileobj(io.BytesIO(data))
    assert s3_etag == blob.e_tag.strip("\"")
    return s3_etag, gs_crc32c

def _upload_multipart(filepath: str, bucket: str, key: str, part_size: int):
    with MultipartUploader(bucket, key) as uploader:
        part_number = 0
        crc32c = checksum.crc32c(b"")
        with open(filepath, "rb") as fh:
            while True:
                data = fh.read(part_size)
                if not data:
                    break
                crc32c.update(data)
                uploader.put_part(part_number, data)
                part_number += 1
    return uploader.s3_etag, crc32c.google_storage_crc32c()

class MultipartUploader:
    def __init__(self, bucket_name: str, key: str):
        self.bucket_name = bucket_name
        self.key = key
        self.mpu = aws.client("s3").create_multipart_upload(Bucket=bucket_name, Key=key)['UploadId']
        self.parts: List[Any] = list()
        self.s3_etag: Optional[str] = None
        self._closed = False

    def put_part(self, part_number: int, data: bytes):
        aws_part_number = part_number + 1
        resp = aws.client("s3").upload_part(
            Body=data,
            Bucket=self.bucket_name,
            Key=self.key,
            PartNumber=aws_part_number,
            UploadId=self.mpu,
        )
        computed_etag = checksum.md5(data).hexdigest()
        assert computed_etag == resp['ETag'].strip("\"")
        self.parts.append(dict(ETag=resp['ETag'], PartNumber=aws_part_number))

    def close(self):
        if not self._closed:
            self._closed = True
            aws.client("s3").complete_multipart_upload(Bucket=self.bucket_name,
                                                       Key=self.key,
                                                       MultipartUpload=dict(Parts=self.parts),
                                                       UploadId=self.mpu)
            bin_md5 = b"".join([checksum.binascii.unhexlify(part['ETag'].strip("\""))
                                for part in self.parts])
            composite_etag = checksum.md5(bin_md5).hexdigest() + "-" + str(len(self.parts))
            assert composite_etag == aws.resource("s3").Bucket(self.bucket_name).Object(self.key).e_tag.strip("\"")
            self.s3_etag = composite_etag

    def __enter__(self, *args, **kwargs):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()
