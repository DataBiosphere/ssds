import io
import os
import warnings
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from typing import Tuple, Generator

import gs_chunked_io as gscio
from google.cloud.storage import Client

from ssds import checksum
from ssds.blobstore.s3 import get_s3_multipart_chunk_size
from ssds.blobstore import BlobStore, AsyncPartIterator, Part, MultipartWriter


class GSBlobStore(BlobStore):
    schema = "gs://"

    def upload_object(self, filepath: str, bucket: str, key: str):
        size = os.stat(filepath).st_size
        chunk_size = get_s3_multipart_chunk_size(size)
        if chunk_size >= size:
            s3_etag, gs_crc32c = self._upload_oneshot(filepath, bucket, key)
        else:
            s3_etag, gs_crc32c = _upload_multipart(filepath, bucket, key, chunk_size)
        blob = _client().bucket(bucket).blob(key)
        blob.metadata = dict(SSDS_MD5=s3_etag, SSDS_CRC32C=gs_crc32c)
        blob.patch()

    def _upload_oneshot(self, filepath: str, bucket: str, key: str) -> Tuple[str, str]:
        with open(filepath, "rb") as fh:
            data = fh.read()
            gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
            s3_etag = checksum.md5(data).hexdigest()
            self.put(bucket, key, data)
        assert gs_crc32c == _client().bucket(bucket).get_blob(key).crc32c
        return s3_etag, gs_crc32c

    def list(self, bucket_name: str, prefix="") -> Generator[str, None, None]:
        for blob in _client().bucket(bucket_name).list_blobs(prefix=prefix):
            yield blob.name

    def get(self, bucket_name: str, key: str) -> bytes:
        blob = _client().bucket(bucket_name).get_blob(key)
        fileobj = io.BytesIO()
        blob.download_to_file(fileobj)
        return fileobj.getvalue()

    def put(self, bucket_name: str, key: str, data: bytes):
        blob = _client().bucket(bucket_name).blob(key)
        blob.upload_from_file(io.BytesIO(data))

    def cloud_native_checksum(self, bucket_name: str, key: str) -> str:
        return _client().bucket(bucket_name).get_blob(key).crc32c

    def parts(self, bucket_name: str, key: str, executor: ThreadPoolExecutor=None) -> "GSAsyncPartIterator":
        return GSAsyncPartIterator(bucket_name, key, executor)

    def multipart_writer(self, bucket_name: str, key: str, executor: ThreadPoolExecutor=None) -> "MultipartWriter":
        return GSMultipartWriter(bucket_name, key, executor)

class GSAsyncPartIterator(AsyncPartIterator):
    def __init__(self, bucket_name: str, key: str, executor: ThreadPoolExecutor=None):
        self._blob = _client().bucket(bucket_name).get_blob(key)
        self.size = self._blob.size
        self.chunk_size = get_s3_multipart_chunk_size(self.size)
        self._number_of_parts = ceil(self.size / self.chunk_size)
        self._executor = executor

    def __iter__(self) -> Generator[Part, None, None]:
        for chunk_number, data in gscio.AsyncReader.for_each_chunk_async(self._blob,
                                                                         self.chunk_size,
                                                                         executor=self._executor):
            yield Part(chunk_number, data)

class GSMultipartWriter(MultipartWriter):
    def __init__(self, bucket_name: str, key: str, executor: ThreadPoolExecutor=None):
        bucket = _client().bucket(bucket_name)
        self._writer = gscio.AsyncWriter(key, bucket, executor=executor)

    def put_part(self, part: Part):
        self._writer.put_part_async(part.number, part.data)

    def close(self):
        self._writer.close()

@lru_cache()
def _client() -> Client:
    if not os.environ.get('GOOGLE_CLOUD_PROJECT'):
        raise RuntimeError("Please set the GOOGLE_CLOUD_PROJECT environment variable")
    # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
    return Client()

def _upload_multipart(filepath: str, bucket_name: str, key: str, part_size: int) -> Tuple[str, str]:
    _crc32c = checksum.crc32c(b"")
    _s3_etags = []

    def _put_part(part_number: int, part_name: str, data: bytes):
        _crc32c.update(bytes(data))
        _s3_etags.append(checksum.md5(data).hexdigest())

    bucket = _client().bucket(bucket_name)
    with gscio.writer.Writer(key, bucket, part_size, part_callback=_put_part) as writer:
        with open(filepath, "rb") as fh:
            while True:
                data = fh.read(part_size)
                if data:
                    writer.write(data)
                else:
                    break

    s3_etag = checksum.compute_composite_etag(_s3_etags)
    gs_crc32c = _crc32c.google_storage_crc32c()
    assert gs_crc32c == bucket.get_blob(key).crc32c
    return s3_etag, gs_crc32c
