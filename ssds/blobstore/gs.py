import io
import os
import warnings
from functools import lru_cache
from math import ceil
from typing import Dict, Optional, Union, Generator

import gs_chunked_io as gscio
from google.cloud.storage import Client

from ssds.blobstore import BlobStore, Blob, AsyncPartIterator, Part, MultipartWriter, get_s3_multipart_chunk_size


class GSBlobStore(BlobStore):
    schema = "gs://"

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def list(self, prefix=""):
        for blob in _client().bucket(self.bucket_name).list_blobs(prefix=prefix):
            yield blob.name

    def blob(self, key: str) -> "GSBlob":
        return GSBlob(self.bucket_name, key)

class GSBlob(Blob):
    def __init__(self, bucket_name: str, key: str):
        self.bucket_name = bucket_name
        self.key = key

    def put_tags(self, tags: Dict[str, str]):
        blob = _client().bucket(self.bucket_name).get_blob(self.key)
        blob.metadata = tags
        blob.patch()

    def get_tags(self) -> Dict[str, str]:
        blob = _client().bucket(self.bucket_name).get_blob(self.key)
        if blob.metadata is None:
            return dict()
        else:
            return blob.metadata.copy()

    def get(self) -> bytes:
        blob = _client().bucket(self.bucket_name).get_blob(self.key)
        fileobj = io.BytesIO()
        blob.download_to_file(fileobj)
        return fileobj.getvalue()

    def put(self, data: bytes):
        blob = _client().bucket(self.bucket_name).blob(self.key)
        blob.upload_from_file(io.BytesIO(data))

    def exists(self) -> bool:
        blob = _client().bucket(self.bucket_name).blob(self.key)
        return blob.exists()

    def size(self) -> int:
        return _client().bucket(self.bucket_name).get_blob(self.key).size

    def cloud_native_checksum(self) -> str:
        return _client().bucket(self.bucket_name).get_blob(self.key).crc32c

    def parts(self, threads: Optional[int]=None) -> "GSAsyncPartIterator":
        return GSAsyncPartIterator(self.bucket_name, self.key, threads)

    def multipart_writer(self, threads: Optional[int]=None) -> "MultipartWriter":
        return GSMultipartWriter(self.bucket_name, self.key, threads)

class GSAsyncPartIterator(AsyncPartIterator):
    def __init__(self, bucket_name: str, key: str, threads: Optional[int]=None):
        self._blob = _client().bucket(bucket_name).get_blob(key)
        self.size = self._blob.size
        self.chunk_size = get_s3_multipart_chunk_size(self.size)
        self._number_of_parts = ceil(self.size / self.chunk_size) if 0 < self.size else 1
        self._threads = threads

    def __iter__(self) -> Generator[Part, None, None]:
        if 1 == self._number_of_parts:
            # TODO: remove this branch when gs-chunked-io supports zero byte files
            data = io.BytesIO()
            self._blob.download_to_file(data)
            yield Part(0, data.getvalue())
        else:
            for chunk_number, data in gscio.for_each_chunk_async(self._blob,
                                                                 self.chunk_size,
                                                                 threads=self._threads):
                yield Part(chunk_number, data)

class _MonkeyPatchedPartUploader(gscio.Writer):
    def put_part(self, part_number: int, data: bytes):
        super()._put_part(part_number, data)

class GSMultipartWriter(MultipartWriter):
    def __init__(self, bucket_name: str, key: str, threads: Optional[int]=None):
        super().__init__()
        bucket = _client().bucket(bucket_name)
        if threads is None:
            self._part_uploader: Union[gscio.AsyncPartUploader, _MonkeyPatchedPartUploader] = \
                _MonkeyPatchedPartUploader(key, bucket, threads)
        else:
            self._part_uploader = gscio.AsyncPartUploader(key, bucket, threads=threads)

    def put_part(self, part: Part):
        self._part_uploader.put_part(part.number, part.data)

    def close(self):
        self._part_uploader.close()

@lru_cache()
def _client() -> Client:
    # Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
    warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")
    return Client()
