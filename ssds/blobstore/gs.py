import io
from math import ceil
from typing import Dict, Optional, Union, Generator

import gs_chunked_io as gscio
from google.cloud.storage import Blob as GSNativeBlob, Bucket as GSNativeBucket
from google.api_core import exceptions as gcp_exceptions

from ssds import gcp, concurrency, utils
from ssds.blobstore import (BlobStore, Blob, AsyncPartIterator, Part, MultipartWriter, get_s3_multipart_chunk_size,
                            BlobNotFoundError, BlobStoreUnknownError)


class GSBlobStore(BlobStore):
    schema = "gs://"

    def __init__(self, bucket_name: str, billing_project: Optional[str]=None):
        self.bucket_name = bucket_name
        self.billing_project = gcp.resolve_billing_project(billing_project)

    def list(self, prefix="") -> Generator["GSBlob", None, None]:
        kwargs = dict()
        if self.billing_project is not None:
            kwargs['user_project'] = self.billing_project
        for blob in gcp.storage_client().bucket(self.bucket_name, **kwargs).list_blobs(prefix=prefix):
            yield GSBlob(self.bucket_name, blob.name, self.billing_project)

    def blob(self, key: str) -> "GSBlob":
        return GSBlob(self.bucket_name, key, self.billing_project)

def _get_native_bucket(bucket: Union[str, GSNativeBucket], billing_project: Optional[str]=None) -> GSNativeBucket:
    if isinstance(bucket, str):
        kwargs = dict()
        if billing_project is not None:
            kwargs['user_project'] = billing_project
        bucket = gcp.storage_client().bucket(bucket, **kwargs)
    return bucket

def _get_native_blob(bucket: Union[str, GSNativeBucket], key: str, billing_project: Optional[str]=None) -> GSNativeBlob:
    bucket = _get_native_bucket(bucket)
    blob = bucket.get_blob(key)
    if blob is None:
        raise BlobNotFoundError(f"Could not find gs://{bucket.name}/{key}")
    return blob

class GSBlob(Blob):
    def __init__(self, bucket_name: str, key: str, billing_project: Optional[str]=None):
        self.bucket_name = bucket_name
        self.key = key
        self.billing_project = gcp.resolve_billing_project(billing_project)
        self._gs_bucket = _get_native_bucket(bucket_name, billing_project)

    @property
    def url(self) -> str:
        return f"{GSBlobStore.schema}{self.bucket_name}/{self.key}"

    def _get_native_blob(self) -> GSNativeBlob:
        return _get_native_blob(self._gs_bucket, self.key)

    @utils.retry(gcp_exceptions.ServiceUnavailable, gcp_exceptions.NotFound)
    def put_tags(self, tags: Dict[str, str]):
        blob = self._get_native_blob()
        blob.metadata = tags
        blob.patch()

    def get_tags(self) -> Dict[str, str]:
        blob = self._get_native_blob()
        if blob.metadata is None:
            return dict()
        else:
            return blob.metadata.copy()

    def get(self) -> bytes:
        return self._get_native_blob().download_as_bytes(checksum=None)

    def put(self, data: bytes):
        blob = self._gs_bucket.blob(self.key)
        blob.upload_from_file(io.BytesIO(data))

    def delete(self):
        self._get_native_blob().delete()

    def copy_from_is_multipart(self, src_blob: "GSBlob") -> bool:
        # FIXME: Does gs even support multipart? Why would the user_project reflect that?
        #        https://github.com/DataBiosphere/ssds/issues/221
        src_gs_blob = src_blob._get_native_blob()
        return src_gs_blob.bucket.user_project is not None

    def copy_from(self, src_blob: "GSBlob"):
        """
        Intra-cloud copy
        """
        assert isinstance(src_blob, type(self))
        if self.url != src_blob.url:
            if not src_blob._gs_bucket.user_project:
                # TODO: always use rewrite when it support requester pays buckets
                dst_gs_blob = self._gs_bucket.blob(self.key)
                src_gs_blob = src_blob._gs_bucket.blob(src_blob.key)
                token: Optional[str] = None
                while True:
                    try:
                        resp = dst_gs_blob.rewrite(src_gs_blob, token)
                    except gcp_exceptions.NotFound:
                        raise BlobNotFoundError(f"Could not find {src_blob.url}")
                    if resp[0] is None:
                        break
                    else:
                        token = resp[0]
            else:
                with self.multipart_writer() as writer:
                    for part in src_blob.parts():
                        writer.put_part(part)

    def download(self, path: str):
        self._get_native_blob().download_to_filename(path)

    def exists(self) -> bool:
        blob = self._gs_bucket.blob(self.key)
        return blob.exists()

    def size(self) -> int:
        return self._get_native_blob().size

    def cloud_native_checksum(self) -> str:
        return self._get_native_blob().crc32c

    def parts(self) -> "GSAsyncPartIterator":
        return GSAsyncPartIterator(self.bucket_name, self.key, self.billing_project)

    def multipart_writer(self) -> "MultipartWriter":
        return GSMultipartWriter(self.bucket_name, self.key, billing_project=self.billing_project)

class GSAsyncPartIterator(AsyncPartIterator):
    def __init__(self, bucket_name: str, key: str, billing_project: Optional[str]=None):
        self._blob = _get_native_blob(bucket_name, key, billing_project)
        self.size = self._blob.size
        self.chunk_size = get_s3_multipart_chunk_size(self.size)
        self._number_of_parts = ceil(self.size / self.chunk_size) if 0 < self.size else 1

    def __iter__(self) -> Generator[Part, None, None]:
        if 1 == self._number_of_parts:
            # TODO: remove this branch when gs-chunked-io supports zero byte files
            data = io.BytesIO()
            self._blob.download_to_file(data)
            yield Part(0, data.getvalue())
        else:
            for chunk_number, data in gscio.for_each_chunk_async(self._blob,
                                                                 concurrency.async_set(),
                                                                 self.chunk_size):
                yield Part(chunk_number, data)

class GSMultipartWriter(MultipartWriter):
    def __init__(self, bucket_name: str, key: str, billing_project: Optional[str]=None):
        super().__init__()
        kwargs = dict()
        if billing_project is not None:
            kwargs['user_project'] = billing_project
        bucket = gcp.storage_client().bucket(bucket_name, **kwargs)
        self._part_uploader = gscio.AsyncPartUploader(key, bucket, concurrency.async_set())

    def put_part(self, part: Part):
        self._part_uploader.put_part(part.number, part.data)

    def close(self):
        self._part_uploader.close()
