"""
Low level cloud agnostic storage API
"""
import os
import logging
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union

from ssds import checksum
from ssds.blobstore import get_s3_multipart_chunk_size, Blob
from ssds.blobstore.s3 import S3BlobStore, S3Blob
from ssds.blobstore.gs import GSBlobStore, GSBlob
from ssds.blobstore.local import LocalBlobStore, LocalBlob
from ssds.concurrency import async_set


logger = logging.getLogger(__name__)

AnyBlobStore = Union[LocalBlobStore, S3BlobStore, GSBlobStore]
AnyBlob = Union[LocalBlob, S3Blob, GSBlob]
CloudBlob = Union[S3Blob, GSBlob]

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
        self._completed_keys: Set[str] = set()

    def copy(self, src_blob: AnyBlob, dst_blob: AnyBlob):
        """
        Copy from `src_blob` to `dst_blob`
        This avoids data passthrough when possible, e.g. S3->S3 or GS->GS. For GS->GS copies, passthrough may be forced
        if the source bucket is requester pays. Checksums are computed for Local->Cloud copies.
        """
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

    def copy_compute_checksums(self, src_blob: AnyBlob, dst_blob: CloudBlob):
        """
        Copy from `src_blob` to `dst_blob`, computing checksums
        This always causes data to pass through the executing instance.
        """
        def _do_oneshot_copy():
            tags = copy_oneshot_passthrough(src_blob, dst_blob, compute_checksums=True)
            self._finalize_copy(src_blob, dst_blob, tags)

        size = src_blob.size()
        if size <= get_s3_multipart_chunk_size(size):
            self._async_set.put(_do_oneshot_copy)
        else:
            tags = copy_multipart_passthrough(src_blob, dst_blob, compute_checksums=True)
            self._async_set.put(self._finalize_copy, src_blob, dst_blob, tags)

    def _download(self, src_blob: AnyBlob, dst_blob: LocalBlob):
        dirname = os.path.dirname(dst_blob.url)
        if dirname:
            os.makedirs(dirname, exist_ok=True)

        def do_download():
            src_blob.download(dst_blob.url)
            self._finalize_copy(src_blob, dst_blob)

        self._async_set.put(do_download)

    def _copy_intra_cloud(self, src_blob: AnyBlob, dst_blob: AnyBlob):
        assert isinstance(src_blob, type(dst_blob))

        def do_oneshot_copy():
            dst_blob.copy_from(src_blob)  # type: ignore
            self._finalize_copy(src_blob, dst_blob)

        if dst_blob.copy_from_is_multipart(src_blob):  # type: ignore
            dst_blob.copy_from(src_blob)  # type: ignore
            self._async_set.put(self._finalize_copy, src_blob, dst_blob)
        else:
            self._async_set.put(do_oneshot_copy)

    def _copy_oneshot(self, src_blob: AnyBlob, dst_blob: CloudBlob):
        assert not isinstance(src_blob, type(dst_blob))

        def do_copy():
            tags = copy_oneshot_passthrough(src_blob, dst_blob, compute_checksums=isinstance(src_blob, LocalBlob))
            self._finalize_copy(src_blob, dst_blob, tags)

        self._async_set.put(do_copy)

    def _copy_multipart(self, src_blob: AnyBlob, dst_blob: CloudBlob):
        assert not isinstance(src_blob, type(dst_blob))
        tags = copy_multipart_passthrough(src_blob, dst_blob, compute_checksums=isinstance(src_blob, LocalBlob))
        self._async_set.put(self._finalize_copy, src_blob, dst_blob, tags)

    def _finalize_copy(self, src_blob: AnyBlob, dst_blob: AnyBlob, tags: Optional[dict]=None):
        if not isinstance(dst_blob, LocalBlob):
            tags = tags or src_blob.get_tags()
            verify_checksums(src_blob.url, dst_blob, tags, self._ignore_missing_checksums)
            dst_blob.put_tags(tags)
        self._completed_keys.add(dst_blob.key)
        logger.info(f"Copied {src_blob.url} to {dst_blob.url}")

    def completed(self) -> Generator[str, None, None]:
        while self._completed_keys:
            yield self._completed_keys.pop()

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        for _ in self._async_set.consume():
            pass

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

def blob_for_url(url: str) -> AnyBlob:
    assert url
    blob: AnyBlob
    if url.startswith("s3://"):
        bucket_name, key = url[5:].split("/", 1)
        blob = S3Blob(bucket_name, key)
    elif url.startswith("gs://"):
        bucket_name, key = url[5:].split("/", 1)
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
        bucket_name, pfx = url[5:].split("/", 1)
        blobstore = S3BlobStore(bucket_name)
    elif url.startswith("gs://"):
        bucket_name, pfx = url[5:].split("/", 1)
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
        bucket_name, pfx = url[5:].split("/", 1)
        listing = S3BlobStore(bucket_name).list(pfx.strip("/"))
    elif url.startswith("gs://"):
        bucket_name, pfx = url[5:].split("/", 1)
        listing = GSBlobStore(bucket_name).list(pfx.strip("/"))
    else:
        pfx = os.path.realpath(os.path.normpath(url)).strip("/")
        listing = LocalBlobStore("/").list(pfx)
    return pfx, listing
