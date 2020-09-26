import os
import sys
import logging
import contextlib
from typing import Tuple, Dict, Union, Optional, Iterable, Generator, Type

from gs_chunked_io.async_collections import AsyncSet

from ssds import checksum
from ssds.blobstore import Blob, BlobStore, get_s3_multipart_chunk_size, Part
from ssds.blobstore.s3 import S3Blob, S3BlobStore
from ssds.blobstore.gs import GSBlob, GSBlobStore
from ssds.blobstore.local import LocalBlob, LocalBlobStore
from ssds.concurrency import async_set, async_queue


logger = logging.getLogger(__name__)

MAX_KEY_LENGTH = 1024  # this is the maximum length for S3 and GS object names
# GS docs: https://cloud.google.com/storage/docs/naming-objects
# S3 docs: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

class SSDSObjectTag:
    SSDS_MD5 = "SSDS_MD5"
    SSDS_CRC32C = "SSDS_CRC32C"

class SSDS:
    blobstore_class: Type[BlobStore]
    bucket: str
    prefix = "submissions"
    _name_delimeter = "--"  # Not using "/" as name delimeter produces friendlier `aws s3` listing

    def __init__(self, google_billing_project: Optional[str]=None):
        kwargs = dict()
        if google_billing_project is not None:
            if self.blobstore_class != GSBlobStore:
                raise ValueError("google_billing_project may only be passed in for Google Storage deployments")
            kwargs['billing_project'] = google_billing_project
        # TODO: figure out how to use type checking on this line
        self.blobstore = self.blobstore_class(self.bucket, **kwargs)  # type: ignore

    def list(self) -> Generator[Tuple[str, str], None, None]:
        listing = self.blobstore.list(self.prefix)
        prev_submission_id = ""
        for key in listing:
            try:
                ssds_key = key.strip(f"{self.prefix}/")
                submission_id, parts = ssds_key.split(self._name_delimeter, 1)
                submission_name, _ = parts.split("/", 1)
            except ValueError:
                continue
            if submission_id != prev_submission_id:
                yield submission_id, submission_name
                prev_submission_id = submission_id

    def __repr__(self) -> str:
        return self.__class__.__name__

    def __str__(self) -> str:
        return self.__repr__()

    def list_submission(self, submission_id: str) -> Generator[str, None, None]:
        for blob in self.blobstore.list(f"{self.prefix}/{submission_id}"):
            ssds_key = blob.key.replace(f"{self.prefix}/", "", 1)
            yield ssds_key

    def get_submission_name(self, submission_id: str) -> Optional[str]:
        name = None
        for blob in self.blobstore.list(f"{self.prefix}/{submission_id}"):
            ssds_key = blob.key.strip(f"{self.prefix}/")
            _, parts = ssds_key.split(self._name_delimeter, 1)
            name, _ = parts.split("/", 1)
            break
        return name

    def upload(self,
               src_url: str,
               submission_id: str,
               name: Optional[str]=None,
               subdir: Optional[str]=None):
        """
        Upload files from src_url directory and yield ssds_key for each file.
        This returns a generator that must be iterated for uploads to occur.
        """
        name = self._check_name_exists(submission_id, name)
        pfx, listing = listing_for_url(src_url)
        for ssds_key in self._upload_tree(listing, pfx, submission_id, name, subdir):
            yield ssds_key

    def copy(self, src_url: str, submission_id: str, name: str, submission_path: str):
        """
        Copy files from local or cloud locations into the ssds.
        """
        src_blob: Union[S3Blob, GSBlob, LocalBlob]
        name = self._check_name_exists(submission_id, name)
        blob = blob_for_url(src_url)
        size = blob.size()
        ssds_key = self._compose_ssds_key(submission_id, name, submission_path)
        part_size = get_s3_multipart_chunk_size(size)
        if part_size >= size:
            self._upload_oneshot(blob, ssds_key)
        else:
            self._upload_multipart(blob, ssds_key, part_size)

    def _check_name_exists(self, submission_id: str, name: Optional[str]) -> str:
        existing_name = self.get_submission_name(submission_id)
        if not name:
            if not existing_name:
                raise ValueError("Must provide name for new submissions")
            name = existing_name
        elif existing_name and existing_name != name:
            raise ValueError("Cannot update name of existing submission")
        return name

    def _upload_tree(self,
                     listing: Iterable[Blob],
                     pfx: str,
                     submission_id: str,
                     name: str,
                     subdir: Optional[str]=None) -> Generator[str, None, None]:
        assert " " not in name  # TODO: create regex to enforce name format?
        assert self._name_delimeter not in name  # TODO: create regex to enforce name format?

        subdir = f"{subdir.strip('/')}" if subdir else ""
        oneshot_uploads = async_set()
        for blob in listing:
            if oneshot_uploads is not None:
                for ssds_key in oneshot_uploads.consume_finished():
                    yield ssds_key
            dst_key = blob.key.replace(pfx.strip("/"), subdir, 1)
            ssds_key = self._compose_ssds_key(submission_id, name, dst_key)
            size = blob.size()
            part_size = get_s3_multipart_chunk_size(size)
            if part_size >= size:
                if oneshot_uploads is not None:
                    oneshot_uploads.put(self._upload_oneshot, blob, ssds_key)
                else:
                    yield self._upload_oneshot(blob, ssds_key)
            else:
                yield self._upload_multipart(blob, ssds_key, part_size)
        if oneshot_uploads is not None:
            for ssds_key in oneshot_uploads.consume():
                yield ssds_key

    def _compose_ssds_key(self, submission_id: str, submission_name: str, path: str) -> str:
        path = path.strip("/")
        dst_prefix = f"{submission_id}{self._name_delimeter}{submission_name}"
        ssds_key = f"{dst_prefix}/{path}"
        blobstore_key = f"{self.prefix}{ssds_key}"
        if MAX_KEY_LENGTH <= len(blobstore_key):
            raise ValueError(f"Total key length must not exceed {MAX_KEY_LENGTH} characters {os.linesep}"
                             f"{blobstore_key} is too long {os.linesep}"
                             f"Use a shorter submission name")
        return ssds_key

    def _upload_oneshot(self, src_blob: Blob, ssds_key: str) -> str:
        data = src_blob.get()
        dst_key = f"{self.prefix}/{ssds_key}"
        gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
        s3_etag = checksum.md5(data).hexdigest()
        self.blobstore.blob(dst_key).put(data)
        tags = {SSDSObjectTag.SSDS_MD5: s3_etag, SSDSObjectTag.SSDS_CRC32C: gs_crc32c}
        self.blobstore.blob(dst_key).put_tags(tags)
        return ssds_key

    def _upload_multipart(self,
                          src_blob: Blob,
                          ssds_key: str,
                          part_size: int) -> str:
        checksums: Dict[str, Union[str, checksum.UnorderedChecksum]]
        if isinstance(src_blob, S3Blob):
            checksums = dict(s3=checksum.S3EtagUnordered(), gs=checksum.GScrc32cUnordered())
        elif isinstance(src_blob, GSBlob):
            checksums = dict(s3=checksum.S3EtagUnordered(), gs=src_blob.cloud_native_checksum())
        elif isinstance(src_blob, LocalBlob):
            checksums = dict(s3=checksum.S3EtagUnordered(), gs=checksum.GScrc32cUnordered())
        else:
            raise TypeError(f"Unknown blob type {type(src_blob)}")
        key = f"{self.prefix}/{ssds_key}"
        with self.blobstore.blob(key).multipart_writer() as uploader:
            for part in src_blob.parts():
                for cs in checksums.values():
                    if isinstance(cs, checksum.UnorderedChecksum):
                        cs.update(part.number, part.data)
                uploader.put_part(part)

        for platform, cs in checksums.items():
            if isinstance(cs, checksum.UnorderedChecksum):
                checksums[platform] = cs.hexdigest()

        def _tag():
            if self.blobstore_class == S3BlobStore:
                assert checksums['s3'] == self.blobstore.blob(key).cloud_native_checksum()
            if self.blobstore_class == GSBlobStore:
                assert checksums['gs'] == self.blobstore.blob(key).cloud_native_checksum()
            tags = {SSDSObjectTag.SSDS_MD5: checksums['s3'], SSDSObjectTag.SSDS_CRC32C: checksums['gs']}
            self.blobstore.blob(key).put_tags(tags)

        # TODO: parallelize tagging
        _tag()
        return ssds_key

    def compose_blobstore_url(self, ssds_key: str) -> str:
        return f"{self.blobstore.schema}{self.bucket}/{self.prefix}/{ssds_key}"

def sync(submission_id: str, src: SSDS, dst: SSDS) -> Generator[str, None, None]:
    def _verify_and_tag(key: str):
        src_tags = src.blobstore.blob(key).get_tags()
        dst_checksum = dst.blobstore.blob(key).cloud_native_checksum()
        if "gs://" == dst.blobstore.schema:
            assert src_tags[SSDSObjectTag.SSDS_CRC32C] == dst_checksum
        elif "s3://" == dst.blobstore.schema:
            assert src_tags[SSDSObjectTag.SSDS_MD5] == dst_checksum
        else:
            raise RuntimeError("Unknown blobstore schema!")
        dst.blobstore.blob(key).put_tags(src_tags)

    def _already_synced(key: str) -> bool:
        if not dst.blobstore.blob(key).exists():
            return False
        else:
            src_tags = src.blobstore.blob(key).get_tags()
            dst_tags = dst.blobstore.blob(key).get_tags()
            if dst_tags != src_tags:
                return False
            else:
                return True

    def _sync_oneshot(key: str, data: bytes) -> Optional[str]:
        if not _already_synced(key):
            logger.info(f"syncing {key} from {src} to {dst}")
            dst.blobstore.blob(key).put(data)
            _verify_and_tag(key)
            return key
        else:
            logger.info(f"already-synced {key} from {src} to {dst}")
            return None

    oneshot_uploads = async_set()
    for blob in src.blobstore.list(f"{src.prefix}/{submission_id}"):
        parts = src.blobstore.blob(blob.key).parts()
        if 1 == len(parts):
            oneshot_uploads.put(_sync_oneshot, blob.key, list(parts)[0].data)
        else:
            if not _already_synced(blob.key):
                logger.info(f"syncing {blob.key} from {src} to {dst}")
                with dst.blobstore.blob(blob.key).multipart_writer() as writer:
                    for part in parts:
                        writer.put_part(part)
                _verify_and_tag(blob.key)
                yield blob.key
            else:
                logger.info(f"already-synced {blob.key} from {src} to {dst}")
        for synced_key in oneshot_uploads.consume_finished():
            yield synced_key
    for synced_key in oneshot_uploads.consume():
        if synced_key is not None:
            yield synced_key

def blob_for_url(url: str) -> Union[LocalBlob, S3Blob, GSBlob]:
    assert url
    blob: Union[S3Blob, GSBlob, LocalBlob]
    if url.startswith("s3://"):
        bucket_name, key = url[5:].split("/", 1)
        blob = S3Blob(bucket_name, key)
    elif url.startswith("gs://"):
        bucket_name, key = url[5:].split("/", 1)
        blob = GSBlob(bucket_name, key)
    else:
        blob = LocalBlob("/", os.path.realpath(os.path.normpath(url)))
    return blob

def blobstore_for_url(url: str) -> Tuple[str, Union[S3BlobStore, GSBlobStore, LocalBlobStore]]:
    """
    url is expected to be a prefix, NOT a key
    """
    blobstore: Union[S3BlobStore, GSBlobStore, LocalBlobStore]
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
