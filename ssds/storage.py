"""
Low level cloud agnostic storage API
"""
import os
import logging
import traceback
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union, Callable

from ssds import checksum
from ssds.blobstore import get_s3_multipart_chunk_size, Blob, BlobNotFoundError
from ssds.blobstore.s3 import S3BlobStore, S3Blob
from ssds.blobstore.gs import GSBlobStore, GSBlob
from ssds.blobstore.local import LocalBlobStore, LocalBlob
from ssds.concurrency import async_set


logger = logging.getLogger(__name__)

AnyBlobStore = Union[LocalBlobStore, S3BlobStore, GSBlobStore]
AnyBlob = Union[LocalBlob, S3Blob, GSBlob]
CloudBlob = Union[S3Blob, GSBlob]
_CopyMethod = Callable[..., Optional[Dict[str, str]]]

class SSDSObjectTag:
    SSDS_MD5 = "SSDS_MD5"
    SSDS_CRC32C = "SSDS_CRC32C"

class SSDSCopyError(Exception):
    pass

class SSDSMissingChecksum(SSDSCopyError):
    pass

class SSDSIncorrectChecksum(SSDSCopyError):
    pass

class CopyClient:
    def __init__(self, ignore_missing_checksums: bool=False):
        self._ignore_missing_checksums = ignore_missing_checksums
        self._async_set = async_set(10)
        self._completed: Set[Tuple[AnyBlob, AnyBlob, Optional[Exception]]] = set()

    def copy(self, src_blob: AnyBlob, dst_blob: AnyBlob):
        """
        Copy from `src_blob` to `dst_blob`
        This avoids data passthrough when possible, e.g. S3->S3 or GS->GS. For GS->GS copies, passthrough may be forced
        if the source bucket is requester pays. Checksums are computed for Local->Cloud copies.
        """
        if src_blob.exists():
            if isinstance(dst_blob, LocalBlob):
                self._download(src_blob, dst_blob)
            elif isinstance(src_blob, type(dst_blob)):
                self._copy_intra_cloud(src_blob, dst_blob)
            else:
                size = src_blob.size()
                if size <= get_s3_multipart_chunk_size(size):
                    self._copy_oneshot(src_blob, dst_blob)
                else:
                    self._copy_multipart(src_blob, dst_blob)
        else:
            logger.error(f"Failed to copy {src_blob.url} to {dst_blob.url}"
                         f"{os.linesep}Source does not exist!")

    def copy_compute_checksums(self, src_blob: AnyBlob, dst_blob: CloudBlob):
        """
        Copy from `src_blob` to `dst_blob`, computing checksums
        This always causes data to pass through the executing instance.
        """
        size = src_blob.size()
        if size <= get_s3_multipart_chunk_size(size):
            self._do_copy_async(copy_oneshot_passthrough, src_blob, dst_blob, compute_checksums=True)
        else:
            self._do_copy(copy_multipart_passthrough, src_blob, dst_blob, compute_checksums=True)

    def _download(self, src_blob: AnyBlob, dst_blob: LocalBlob):
        dirname = os.path.dirname(dst_blob.url)
        if dirname:
            os.makedirs(dirname, exist_ok=True)
        self._do_copy_async(_copy_to_local, src_blob, dst_blob)

    def _copy_intra_cloud(self, src_blob: AnyBlob, dst_blob: AnyBlob):
        if dst_blob.copy_from_is_multipart(src_blob):  # type: ignore
            self._do_copy(_copy_intra_cloud, src_blob, dst_blob)
        else:
            self._do_copy_async(_copy_intra_cloud, src_blob, dst_blob)

    def _copy_oneshot(self, src_blob: AnyBlob, dst_blob: CloudBlob):
        self._do_copy_async(copy_oneshot_passthrough,
                            src_blob,
                            dst_blob,
                            compute_checksums=isinstance(src_blob, LocalBlob))

    def _copy_multipart(self, src_blob: AnyBlob, dst_blob: CloudBlob):
        self._do_copy(copy_multipart_passthrough,
                      src_blob,
                      dst_blob,
                      compute_checksums=isinstance(src_blob, LocalBlob))

    def _do_copy(self, copy_func: _CopyMethod, src_blob: AnyBlob, dst_blob: AnyBlob, *args, **kwargs):
        try:
            tags = copy_func(src_blob, dst_blob, *args, **kwargs)
            if not isinstance(dst_blob, LocalBlob):
                tags = tags or src_blob.get_tags()
                verify_checksums(src_blob.url, dst_blob, tags, self._ignore_missing_checksums)
                dst_blob.put_tags(tags)
            self._completed.add((src_blob, dst_blob, None))
            logger.info(f"Copied {src_blob.url} to {dst_blob.url}")
        except Exception as exception:
            self._completed.add((src_blob, dst_blob, exception))
            logger.error(f"Failed to copy {src_blob.url} to {dst_blob.url}"
                         f"{os.linesep}{traceback.format_exc()}")

    def _do_copy_async(self, copy_func: _CopyMethod, src_blob: AnyBlob, dst_blob: AnyBlob, *args, **kwargs):
        self._async_set.put(self._do_copy, copy_func, src_blob, dst_blob, *args, **kwargs)

    def completed(self) -> Generator[Tuple[AnyBlob, AnyBlob, Optional[Exception]], None, None]:
        while self._completed:
            src_blob, dst_blob, exception = self._completed.pop()
            yield src_blob, dst_blob, exception

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        for _ in self._async_set.consume():
            pass

def _copy_to_local(src_blob: AnyBlob, dst_blob: LocalBlob) -> Dict[str, str]:
    src_blob.download(dst_blob.url)
    return dict()  # return empty tags for downloads

def _copy_intra_cloud(src_blob: AnyBlob, dst_blob: AnyBlob) -> Dict[str, str]:
    assert isinstance(src_blob, type(dst_blob))
    dst_blob.copy_from(src_blob)  # type: ignore
    return src_blob.get_tags()

def verify_checksums(src_url: str,
                     dst_blob: CloudBlob,
                     checksums: Dict[str, str],
                     ignore_missing_checksums: bool=False):
    checksum_checks = {S3Blob: ("S3 ETag", SSDSObjectTag.SSDS_MD5),
                       GSBlob: ("GS crc32c", SSDSObjectTag.SSDS_CRC32C)}
    if not isinstance(dst_blob, LocalBlob):
        cs_name, cs_tag = checksum_checks[dst_blob.__class__]
        if cs_tag in checksums:
            if checksums[cs_tag] != dst_blob.cloud_native_checksum():
                raise SSDSIncorrectChecksum(f"Incorrect {cs_name} for {src_url} -> {dst_blob.url}")
        else:
            msg = f"Missing {cs_tag} tag for {src_url}"
            if ignore_missing_checksums:
                logger.warning(msg)
            else:
                raise SSDSMissingChecksum(msg)

def copy_oneshot_passthrough(src_blob: AnyBlob,
                             dst_blob: CloudBlob,
                             compute_checksums: bool=False) -> Optional[Dict[str, str]]:
    """
    Copy from `src_blob` to `dst_blob`, passing data through the executing instance.
    Optionally compute checksums.
    """
    data = src_blob.get()
    checksums: Optional[dict] = None
    if compute_checksums:
        checksums = {SSDSObjectTag.SSDS_MD5: checksum.md5(data).hexdigest(),
                     SSDSObjectTag.SSDS_CRC32C: checksum.crc32c(data).google_storage_crc32c()}
    dst_blob.put(data)
    return checksums

def copy_multipart_passthrough(src_blob: AnyBlob,
                               dst_blob: CloudBlob,
                               compute_checksums: bool=False) -> Optional[Dict[str, str]]:
    """
    Copy from `src_blob` to `dst_blob`, passing data through the executing instance.
    Optionally compute checksums.
    """
    checksums: Optional[dict] = None
    if compute_checksums:
        checksums = {SSDSObjectTag.SSDS_MD5: checksum.S3EtagUnordered(),
                     SSDSObjectTag.SSDS_CRC32C: checksum.GScrc32cUnordered()}
    with dst_blob.multipart_writer() as writer:
        for part in src_blob.parts():
            if checksums is not None:
                for cs in checksums.values():
                    cs.update(part.number, part.data)
            writer.put_part(part)
    if checksums is not None:
        return {key: cs.hexdigest() for key, cs in checksums.items()}
    else:
        return None

def copy(src_blob: AnyBlob, dst_blob: AnyBlob):
    with CopyClient() as client:
        client.copy(src_blob, dst_blob)

def copy_compute_checksums(src_blob: AnyBlob, dst_blob: CloudBlob):
    with CopyClient() as client:
        client.copy_compute_checksums(src_blob, dst_blob)

def transform_key(src_key: str, src_pfx: str, dst_pfx: str) -> str:
    dst_key = src_key.replace(src_pfx.strip("/"), dst_pfx.strip("/"), 1)
    return dst_key

def parse_cloud_url(url: str) -> Tuple[str, str]:
    if url.startswith("s3://") or url.startswith("gs://"):
        bucket_name, key = url[5:].split("/", 1)
        return bucket_name, key
    else:
        raise ValueError(f"Expected either 'gs://' or 's3://' url, got '{url}'")

def blob_for_url(url: str) -> AnyBlob:
    assert url
    blob: AnyBlob
    if url.startswith("s3://"):
        bucket_name, key = parse_cloud_url(url)
        blob = S3Blob(bucket_name, key)
    elif url.startswith("gs://"):
        bucket_name, key = parse_cloud_url(url)
        blob = GSBlob(bucket_name, key)
    else:
        blob = LocalBlob("/", os.path.realpath(os.path.normpath(url)))
    return blob

def blobstore_for_url(url: str) -> Tuple[str, AnyBlobStore]:
    """
    url is expected to be a prefix, NOT a key
    """
    blobstore: AnyBlobStore
    if url.startswith("s3://"):
        bucket_name, pfx = parse_cloud_url(url)
        blobstore = S3BlobStore(bucket_name)
    elif url.startswith("gs://"):
        bucket_name, pfx = parse_cloud_url(url)
        blobstore = GSBlobStore(bucket_name)
    else:
        pfx = os.path.realpath(os.path.normpath(url.strip(os.path.sep)))
        blobstore = LocalBlobStore("/")
    return pfx, blobstore

def listing_for_url(url: str) -> Tuple[str, Union[Generator[S3Blob, None, None],
                                                  Generator[GSBlob, None, None],
                                                  Generator[LocalBlob, None, None]]]:
    assert url
    listing: Union[Generator[S3Blob, None, None],
                   Generator[GSBlob, None, None],
                   Generator[LocalBlob, None, None]]
    if url.startswith("s3://"):
        bucket_name, pfx = parse_cloud_url(url)
        listing = S3BlobStore(bucket_name).list(pfx.strip("/"))
    elif url.startswith("gs://"):
        bucket_name, pfx = parse_cloud_url(url)
        listing = GSBlobStore(bucket_name).list(pfx.strip("/"))
    else:
        pfx = os.path.realpath(os.path.normpath(url)).strip("/")
        listing = LocalBlobStore("/").list(pfx)
    return pfx, listing
