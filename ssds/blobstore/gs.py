import io
import os
import warnings
from functools import lru_cache
from concurrent.futures import ThreadPoolExecutor
from math import ceil
from typing import Dict, Generator

import gs_chunked_io as gscio
from google.cloud.storage import Client

from ssds.blobstore import BlobStore, AsyncPartIterator, Part, MultipartWriter, get_s3_multipart_chunk_size


class GSBlobStore(BlobStore):
    schema = "gs://"

    def put_tags(self, bucket_name: str, key: str, tags: Dict[str, str]):
        blob = _client().bucket(bucket_name).get_blob(key)
        blob.metadata = tags
        blob.patch()

    def get_tags(self, bucket_name: str, key: str) -> Dict[str, str]:
        blob = _client().bucket(bucket_name).get_blob(key)
        return blob.metadata.copy()

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

    def exists(self, bucket_name: str, key: str) -> bool:
        blob = _client().bucket(bucket_name).blob(key)
        return blob.exists()

    def size(self, bucket_name: str, key: str) -> int:
        return _client().bucket(bucket_name).get_blob(key).size

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
        super().__init__()
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
