import io
import requests
from math import ceil
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Optional, Union, Generator

from gs_chunked_io.async_collections import AsyncSet

import botocore.exceptions

from ssds import aws
from ssds.blobstore import BlobStore, Blob, AsyncPartIterator, Part, MultipartWriter, get_s3_multipart_chunk_size


class S3BlobStore(BlobStore):
    schema = "s3://"

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def list(self, prefix=""):
        for item in aws.resource("s3").Bucket(self.bucket_name).objects.filter(Prefix=prefix):
            yield item.key

    def blob(self, key: str) -> "S3Blob":
        return S3Blob(self.bucket_name, key)

class S3Blob(Blob):
    def __init__(self, bucket_name: str, key: str):
        self.bucket_name = bucket_name
        self._s3_bucket = aws.resource("s3").Bucket(self.bucket_name)
        self.key = key

    def put_tags(self, tags: Dict[str, str]):
        aws_tags = [dict(Key=k, Value=v)
                    for k, v in tags.items()]
        aws.client("s3").put_object_tagging(Bucket=self.bucket_name, Key=self.key, Tagging=dict(TagSet=aws_tags))

    def get_tags(self) -> Dict[str, str]:
        tagset = aws.client("s3").get_object_tagging(Bucket=self.bucket_name, Key=self.key)
        return {tag['Key']: tag['Value']
                for tag in tagset['TagSet']}

    def get(self) -> bytes:
        with closing(self._s3_bucket.Object(self.key).get()['Body']) as fh:
            return fh.read()

    def put(self, data: bytes):
        blob = self._s3_bucket.Object(self.key)
        blob.upload_fileobj(io.BytesIO(data))

    def exists(self) -> bool:
        try:
            self.size()
            return True
        except botocore.exceptions.ClientError as e:
            if str(e.response['Error']['Code']) == str(requests.codes.not_found):
                return False
            else:
                raise

    def size(self) -> int:
        blob = self._s3_bucket.Object(self.key)
        return blob.content_length

    def cloud_native_checksum(self) -> str:
        blob = self._s3_bucket.Object(self.key)
        return blob.e_tag.strip("\"")

    def parts(self, threads: Optional[int]=None) -> "S3AsyncPartIterator":
        return S3AsyncPartIterator(self.bucket_name, self.key, threads)

    def multipart_writer(self, threads: Optional[int]=None) -> "MultipartWriter":
        return S3MultipartWriter(self.bucket_name, self.key, threads)

class S3AsyncPartIterator(AsyncPartIterator):
    def __init__(self, bucket_name, key, threads: Optional[int]=None):
        assert threads and 1 <= threads
        self._blob = aws.resource("s3").Bucket(bucket_name).Object(key)
        self.size = self._blob.content_length
        self.chunk_size = get_s3_multipart_chunk_size(self.size)
        self._number_of_parts = ceil(self.size / self.chunk_size) if 0 < self.size else 1
        self._threads = threads

    def __iter__(self) -> Generator[Part, None, None]:
        if 1 == self._number_of_parts:
            yield self._get_part(0)
        else:
            with ThreadPoolExecutor(max_workers=self._threads) as e:
                parts = AsyncSet(e, concurrency=self._threads)
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
    def __init__(self, bucket_name: str, key: str, threads: Optional[int]=None):
        self.bucket_name = bucket_name
        self.key = key
        self.mpu = aws.client("s3").create_multipart_upload(Bucket=bucket_name, Key=key)['UploadId']
        self.parts: List[Dict[str, Union[str, int]]] = list()
        self._closed = False
        if threads is None:
            self._executor: Optional[ThreadPoolExecutor] = None
            self._part_uploads: Optional[AsyncSet] = None
        else:
            self._executor = ThreadPoolExecutor(max_workers=threads)
            self._part_uploads = AsyncSet(self._executor, concurrency=threads)

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

    def put_part(self, part: Part):
        if self._part_uploads is not None:
            self._collect_parts()
            self._part_uploads.put(self._put_part, part)
        else:
            self.parts.append(self._put_part(part))

    def _collect_parts(self, wait=False):
        if self._part_uploads is not None:
            if wait:
                consumer = self._part_uploads.consume
            else:
                consumer = self._part_uploads.consume_finished
            for part in consumer():
                self.parts.append(part)

    def close(self):
        if not self._closed:
            self._closed = True
            if self._part_uploads is not None:
                self._collect_parts(wait=True)
            if self._executor:
                self._executor.shutdown()
            self.parts.sort(key=lambda item: item['PartNumber'])
            aws.client("s3").complete_multipart_upload(Bucket=self.bucket_name,
                                                       Key=self.key,
                                                       MultipartUpload=dict(Parts=self.parts),
                                                       UploadId=self.mpu)
