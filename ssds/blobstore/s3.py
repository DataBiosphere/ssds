import io
import os
from math import ceil
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Any, Set, List, Dict, Tuple, Union, Optional, Generator

from ssds import aws, checksum
from ssds.blobstore import (MiB, AWS_MIN_CHUNK_SIZE, AWS_MAX_MULTIPART_COUNT, BlobStore, AsyncPartIterator, Part,
                            SSDSObjectTag, MultipartWriter)

class S3BlobStore(BlobStore):
    schema = "s3://"

    def upload_object(self, filepath: str, bucket: str, key: str):
        size = os.stat(filepath).st_size
        chunk_size = get_s3_multipart_chunk_size(size)
        if chunk_size >= size:
            s3_etag, gs_crc32c = self._upload_oneshot(filepath, bucket, key)
        else:
            s3_etag, gs_crc32c = _upload_multipart(filepath, bucket, key, chunk_size)
        assert s3_etag == self.cloud_native_checksum(bucket, key)
        tags = {SSDSObjectTag.SSDS_MD5: s3_etag, SSDSObjectTag.SSDS_CRC32C: gs_crc32c}
        self.put_tags(bucket, key, tags)

    def _upload_oneshot(self, filepath: str, bucket: str, key: str) -> Tuple[str, str]:
        with open(filepath, "rb") as fh:
            data = fh.read()
        gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
        s3_etag = checksum.md5(data).hexdigest()
        self.put(bucket, key, data)
        return s3_etag, gs_crc32c

    def put_tags(self, bucket_name: str, key: str, tags: Dict[str, str]):
        aws_tags = [dict(Key=key, Value=val)
                    for key, val in tags.items()]
        aws.client("s3").put_object_tagging(Bucket=bucket_name, Key=key, Tagging=dict(TagSet=aws_tags))

    def get_tags(self, bucket_name: str, key: str) -> Dict[str, str]:
        tagset = aws.client("s3").get_object_tagging(Bucket=bucket_name, Key=key)
        return {tag['Key']: tag['Value']
                for tag in tagset['TagSet']}

    def list(self, bucket: str, prefix="") -> Generator[str, None, None]:
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

    def multipart_writer(self, bucket_name: str, key: str, executor: ThreadPoolExecutor=None) -> "MultipartWriter":
        return S3MultipartWriter(bucket_name, key, executor)

class S3AsyncPartIterator(AsyncPartIterator):
    parts_to_buffer = 2

    def __init__(self, bucket_name, key, executor: ThreadPoolExecutor=None):
        self._blob = aws.resource("s3").Bucket(bucket_name).Object(key)
        self.size = self._blob.content_length
        self.chunk_size = get_s3_multipart_chunk_size(self.size)
        self._number_of_parts = ceil(self.size / self.chunk_size)
        self._executor = executor or ThreadPoolExecutor(max_workers=4)

    def __iter__(self) -> Generator[Part, None, None]:
        if 1 == self._number_of_parts:
            yield self._get_part(0)
        else:
            futures: Set[Future] = set()
            part_numbers = [part_number for part_number in range(self._number_of_parts)]
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

def get_s3_multipart_chunk_size(filesize: int) -> int:
    """Returns the chunk size of the S3 multipart object, given a file's size."""
    if filesize <= AWS_MAX_MULTIPART_COUNT * AWS_MIN_CHUNK_SIZE:
        return AWS_MIN_CHUNK_SIZE
    else:
        raw_part_size = ceil(filesize / AWS_MAX_MULTIPART_COUNT)
        part_size_in_integer_megabytes = ((raw_part_size + MiB - 1) // MiB) * MiB
        return part_size_in_integer_megabytes

def _upload_multipart(filepath: str, bucket: str, key: str, part_size: int) -> Tuple[str, str]:
    with S3MultipartWriter(bucket, key) as uploader:
        part_number = 0
        crc32c = checksum.crc32c(b"")
        with open(filepath, "rb") as fh:
            while True:
                data = fh.read(part_size)
                if not data:
                    break
                crc32c.update(data)
                uploader.put_part(Part(part_number, data))
                part_number += 1
    return uploader.s3_etag, crc32c.google_storage_crc32c()

class S3MultipartWriter(MultipartWriter):
    concurrent_uploads = 4

    def __init__(self, bucket_name: str, key: str, executor: ThreadPoolExecutor=None):
        self.bucket_name = bucket_name
        self.key = key
        self.mpu = aws.client("s3").create_multipart_upload(Bucket=bucket_name, Key=key)['UploadId']
        self.parts: List[Dict[str, Union[str, int]]] = list()
        self.s3_etag: Optional[str] = None
        self._closed = False
        self._executor = executor
        self._futures: Set[Future] = set()

    def _put_part(self, part: Part) -> Dict[str, Union[str, int]]:
        aws_part_number = part.number + 1
        resp = aws.client("s3").upload_part(
            Body=part.data,
            Bucket=self.bucket_name,
            Key=self.key,
            PartNumber=aws_part_number,
            UploadId=self.mpu,
        )
        computed_etag = checksum.md5(part.data).hexdigest()
        assert computed_etag == resp['ETag'].strip("\"")
        return dict(ETag=resp['ETag'], PartNumber=aws_part_number)

    def put_part(self, part: Part):
        if self._executor:
            if self.concurrent_uploads <= len(self._futures):
                for _ in as_completed(self._futures):
                    break
            self._collect_parts()
            f = self._executor.submit(self._put_part, part)
            self._futures.add(f)
        else:
            self.parts.append(self._put_part(part))

    def _wait(self):
        """
        Wait for current part uploads to finish.
        """
        if self._executor:
            if self._futures:
                for f in as_completed(self._futures):
                    f.result()  # raises if future errored

    def _collect_parts(self):
        if self._executor:
            for f in self._futures.copy():
                if f.done():
                    self.parts.append(f.result())
                    self._futures.remove(f)

    def close(self):
        if not self._closed:
            self._closed = True
            if self._executor:
                self._wait()
                self._collect_parts()
            self.parts.sort(key=lambda item: item['PartNumber'])
            aws.client("s3").complete_multipart_upload(Bucket=self.bucket_name,
                                                       Key=self.key,
                                                       MultipartUpload=dict(Parts=self.parts),
                                                       UploadId=self.mpu)
            bin_md5 = b"".join([checksum.binascii.unhexlify(part['ETag'].strip("\""))
                                for part in self.parts])
            composite_etag = checksum.md5(bin_md5).hexdigest() + "-" + str(len(self.parts))
            self.s3_etag = composite_etag
