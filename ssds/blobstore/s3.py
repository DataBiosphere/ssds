import io
import requests
from math import ceil
from functools import wraps
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Dict, Tuple, Optional, Union, Generator

from gs_chunked_io.async_collections import AsyncSet

import botocore.exceptions

from ssds import aws
from ssds.blobstore import (BlobStore, Blob, AsyncPartIterator, Part, MultipartWriter, get_s3_multipart_chunk_size,
                            BlobNotFoundError, BlobStoreUnknownError)
from ssds.concurrency import async_set


def catch_blob_not_found(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except botocore.exceptions.ClientError as ex:
            if ex.response['Error']['Code'] in [str(requests.codes.not_found),
                                                str(requests.codes.bad_request),
                                                "NoSuchKey"]:
                raise BlobNotFoundError(f"Could not find s3://{self.bucket_name}/{self.key}") from ex
            raise BlobStoreUnknownError(ex)
    return wrapper

class S3BlobStore(BlobStore):
    schema = "s3://"

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def list(self, prefix="") -> Generator["S3Blob", None, None]:
        for item in aws.resource("s3").Bucket(self.bucket_name).objects.filter(Prefix=prefix):
            yield S3Blob(self.bucket_name, item.key)

    def blob(self, key: str) -> "S3Blob":
        return S3Blob(self.bucket_name, key)

class S3Blob(Blob):
    def __init__(self, bucket_name: str, key: str):
        self.bucket_name = bucket_name
        self._s3_bucket = aws.resource("s3").Bucket(self.bucket_name)
        self.key = key

    @property
    def url(self) -> str:
        return f"{S3BlobStore.schema}{self.bucket_name}/{self.key}"

    @catch_blob_not_found
    def put_tags(self, tags: Dict[str, str]):
        aws_tags = [dict(Key=k, Value=v)
                    for k, v in tags.items()]
        aws.client("s3").put_object_tagging(Bucket=self.bucket_name, Key=self.key, Tagging=dict(TagSet=aws_tags))

    @catch_blob_not_found
    def get_tags(self) -> Dict[str, str]:
        tagset = aws.client("s3").get_object_tagging(Bucket=self.bucket_name, Key=self.key)
        return {tag['Key']: tag['Value']
                for tag in tagset['TagSet']}

    @catch_blob_not_found
    def get(self) -> bytes:
        with closing(self._s3_bucket.Object(self.key).get()['Body']) as fh:
            return fh.read()

    def put(self, data: bytes):
        blob = self._s3_bucket.Object(self.key)
        blob.upload_fileobj(io.BytesIO(data))

    def copy_from_is_multipart(self, src_blob: "S3Blob") -> bool:
        size = src_blob.size()
        return size >= get_s3_multipart_chunk_size(size)

    def copy_from(self, src_blob: "S3Blob"):
        """
        Intra-cloud copy
        """
        assert isinstance(src_blob, type(self))
        if self.url != src_blob.url:
            size = src_blob.size()
            part_size = get_s3_multipart_chunk_size(size)
            if part_size >= size:
                self._s3_bucket.Object(self.key).copy_from(CopySource=dict(Bucket=src_blob.bucket_name,
                                                                           Key=src_blob.key))
            else:
                number_of_parts = ceil(size / part_size)
                with self.multipart_writer() as writer:
                    for part_number in range(number_of_parts):
                        writer.put_part_copy(part_number, src_blob)

    @catch_blob_not_found
    def download(self, path: str):
        self._s3_bucket.Object(self.key).download_file(path)

    def exists(self) -> bool:
        try:
            self.size()
            return True
        except BlobNotFoundError:
            return False

    @catch_blob_not_found
    def size(self) -> int:
        blob = self._s3_bucket.Object(self.key)
        return blob.content_length

    @catch_blob_not_found
    def cloud_native_checksum(self) -> str:
        blob = self._s3_bucket.Object(self.key)
        return blob.e_tag.strip("\"")

    def parts(self) -> "S3AsyncPartIterator":
        return S3AsyncPartIterator(self.bucket_name, self.key)

    def multipart_writer(self) -> "MultipartWriter":
        return S3MultipartWriter(self.bucket_name, self.key)

class S3AsyncPartIterator(AsyncPartIterator):
    def __init__(self, bucket_name: str, key: str):
        self._blob, self.size = self._get_blob(bucket_name, key)
        self.chunk_size = get_s3_multipart_chunk_size(self.size)
        self._number_of_parts = ceil(self.size / self.chunk_size) if 0 < self.size else 1

    def _get_blob(self, bucket_name: str, key: str) -> Tuple[Any, int]:
        try:
            blob = aws.resource("s3").Bucket(bucket_name).Object(key)
            size = blob.content_length
        except botocore.exceptions.ClientError as ex:
            if ex.response['Error']['Code'] in [str(requests.codes.not_found), "NoSuchKey"]:
                raise BlobNotFoundError(f"Could not find s3://{bucket_name}/{key}") from ex
            raise BlobStoreUnknownError(ex)
        return blob, size

    def __iter__(self) -> Generator[Part, None, None]:
        if 1 == self._number_of_parts:
            yield self._get_part(0)
        else:
            parts = async_set()
            for part_number in range(self._number_of_parts):
                parts.put(self._get_part, part_number)
                for part in parts.consume_finished():
                    yield part
            for part in parts.consume():
                yield part

    def _get_part(self, part_number: int) -> Part:
        if 1 == self._number_of_parts:
            assert 0 == part_number
            data = self._blob.get()['Body'].read()
        else:
            offset = part_number * self.chunk_size
            byte_range = f"bytes={offset}-{offset + self.chunk_size - 1}"
            data = self._blob.get(Range=byte_range)['Body'].read()
        return Part(part_number, data)

class S3MultipartWriter(MultipartWriter):
    def __init__(self, bucket_name: str, key: str):
        self.bucket_name = bucket_name
        self.key = key
        self.mpu = aws.client("s3").create_multipart_upload(Bucket=bucket_name, Key=key)['UploadId']
        self.parts: List[Dict[str, Union[str, int]]] = list()
        self._closed = False
        self._part_uploads = async_set()

    def _put_part(self, part: Part) -> Dict[str, Union[str, int]]:
        aws_part_number = part.number + 1
        resp = aws.client("s3").upload_part(
            Body=part.data,
            Bucket=self.bucket_name,
            Key=self.key,
            PartNumber=aws_part_number,
            UploadId=self.mpu,
        )
        return dict(ETag=resp['ETag'], PartNumber=aws_part_number)

    def _put_part_copy(self, part_number: int, src_blob: S3Blob):
        aws_part_number = part_number + 1
        size = src_blob.size()
        chunk_size = get_s3_multipart_chunk_size(size)
        start_bytes = part_number * chunk_size
        end_bytes = start_bytes + chunk_size - 1
        if end_bytes >= size:
            end_bytes = size - 1
        resp = aws.client("s3").upload_part_copy(
            Bucket=self.bucket_name,
            Key=self.key,
            PartNumber=aws_part_number,
            CopySource=dict(Bucket=src_blob.bucket_name, Key=src_blob.key),
            CopySourceRange=f"bytes={start_bytes}-{end_bytes}",
            UploadId=self.mpu,
        )
        return dict(ETag=resp['CopyPartResult']['ETag'], PartNumber=aws_part_number)

    def put_part(self, part: Part):
        self._collect_parts()
        self._part_uploads.put(self._put_part, part)

    def put_part_copy(self, part_number: int, src_blob: S3Blob):
        self._collect_parts()
        self._part_uploads.put(self._put_part_copy, part_number, src_blob)

    def _collect_parts(self, wait=False):
        if wait:
            consumer = self._part_uploads.consume
        else:
            consumer = self._part_uploads.consume_finished
        for part in consumer():
            self.parts.append(part)

    def close(self):
        if not self._closed:
            self._closed = True
            self._collect_parts(wait=True)
            self.parts.sort(key=lambda item: item['PartNumber'])
            aws.client("s3").complete_multipart_upload(Bucket=self.bucket_name,
                                                       Key=self.key,
                                                       MultipartUpload=dict(Parts=self.parts),
                                                       UploadId=self.mpu)
