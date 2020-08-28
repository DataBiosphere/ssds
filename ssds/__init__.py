import os
import sys
import logging
import contextlib
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, Dict, Union, Optional, Generator, Type

from gs_chunked_io.async_collections import AsyncSet

from ssds import checksum
from ssds.blobstore import Blob, BlobStore, get_s3_multipart_chunk_size, Part
from ssds.blobstore.s3 import S3Blob, S3BlobStore
from ssds.blobstore.gs import GSBlob, GSBlobStore


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
        for key in self.blobstore.list(f"{self.prefix}/{submission_id}"):
            ssds_key = key.replace(f"{self.prefix}/", "", 1)
            yield ssds_key

    def get_submission_name(self, submission_id: str) -> Optional[str]:
        name = None
        for key in self.blobstore.list(f"{self.prefix}/{submission_id}"):
            ssds_key = key.strip(f"{self.prefix}/")
            _, parts = ssds_key.split(self._name_delimeter, 1)
            name, _ = parts.split("/", 1)
            break
        return name

    def upload(self,
               root: str,
               submission_id: str,
               name: Optional[str]=None,
               threads: Optional[int]=None) -> Generator[str, None, None]:
        """
        Upload files from root directory and yield ssds_key for each file.
        This returns a generator that must be iterated for uploads to occur.
        """
        existing_name = self.get_submission_name(submission_id)
        if not name:
            if not existing_name:
                raise ValueError("Must provide name for new submissions")
            name = existing_name
        elif existing_name and existing_name != name:
            raise ValueError("Cannot update name of existing submission")
        for ssds_key in self._upload_local_tree(root, submission_id, name, threads):
            yield ssds_key

    def copy(self, src_url: str, submission_id: str, name: str, submission_path: str, threads: Optional[int]=None):
        """
        Copy files from local or cloud locations into the ssds.
        """
        if src_url.startswith("s3://"):
            bucket_name, key = src_url[5:].split("/", 1)
            size = S3Blob(bucket_name, key).size()
        elif src_url.startswith("gs://"):
            bucket_name, key = src_url[5:].split("/", 1)
            size = GSBlob(bucket_name, key).size()
        else:
            size = os.path.getsize(src_url)
        ssds_key = self._compose_ssds_key(submission_id, name, submission_path)
        part_size = get_s3_multipart_chunk_size(size)
        if part_size >= size:
            self._upload_oneshot(src_url, ssds_key)
        else:
            self._upload_multipart(src_url, ssds_key, part_size, threads)

    def _upload_local_tree(self,
                           root: str,
                           submission_id: str,
                           name: str,
                           threads: Optional[int]=None) -> Generator[str, None, None]:
        root = os.path.normpath(root)
        assert root == os.path.abspath(root)
        assert " " not in name  # TODO: create regex to enforce name format?
        assert self._name_delimeter not in name  # TODO: create regex to enforce name format?

        if threads is None:
            e: Union[contextlib.AbstractContextManager, ThreadPoolExecutor] = contextlib.nullcontext()
            oneshot_uploads: Optional[AsyncSet] = None
        else:
            e = ThreadPoolExecutor(max_workers=threads)
            oneshot_uploads = AsyncSet(e, concurrency=threads)

        with e:
            for filepath in _list_tree(root):
                if oneshot_uploads is not None:
                    for ssds_key in oneshot_uploads.consume_finished():
                        yield ssds_key
                ssds_key = self._compose_ssds_key(submission_id, name, os.path.relpath(filepath, root))
                size = os.path.getsize(filepath)
                part_size = get_s3_multipart_chunk_size(size)
                if part_size >= size:
                    if oneshot_uploads is not None:
                        oneshot_uploads.put(self._upload_oneshot, filepath, ssds_key)
                    else:
                        yield self._upload_oneshot(filepath, ssds_key)
                else:
                    yield self._upload_multipart(filepath, ssds_key, part_size, threads)
            if oneshot_uploads is not None:
                for ssds_key in oneshot_uploads.consume():
                    yield ssds_key

    def _compose_ssds_key(self, submission_id: str, submission_name: str, path: str) -> str:
        dst_prefix = f"{submission_id}{self._name_delimeter}{submission_name}"
        ssds_key = f"{dst_prefix}/{path}"
        blobstore_key = f"{self.prefix}{ssds_key}"
        if MAX_KEY_LENGTH <= len(blobstore_key):
            raise ValueError(f"Total key length must not exceed {MAX_KEY_LENGTH} characters {os.linesep}"
                             f"{blobstore_key} is too long {os.linesep}"
                             f"Use a shorter submission name")
        return ssds_key

    def _upload_oneshot(self, url: str, ssds_key: str) -> str:
        dst_key = f"{self.prefix}/{ssds_key}"
        if url.startswith("s3://"):
            bucket_name, key = url[5:].split("/", 1)
            data = S3Blob(bucket_name, key).get()
        elif url.startswith("gs://"):
            bucket_name, key = url[5:].split("/", 1)
            data = GSBlob(bucket_name, key).get()
        else:
            with open(url, "rb") as fh:
                data = fh.read()
        gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
        s3_etag = checksum.md5(data).hexdigest()
        self.blobstore.blob(dst_key).put(data)
        tags = {SSDSObjectTag.SSDS_MD5: s3_etag, SSDSObjectTag.SSDS_CRC32C: gs_crc32c}
        self.blobstore.blob(dst_key).put_tags(tags)
        return ssds_key

    def _upload_multipart(self,
                          url: str,
                          ssds_key: str,
                          part_size: int,
                          threads: Optional[int]) -> str:
        checksums: Dict[str, Union[str, checksum.UnorderedChecksum]]
        if url.startswith("s3://"):
            bucket_name, key = url[5:].split("/", 1)
            s3_blob = S3Blob(bucket_name, key)
            parts = s3_blob.parts(threads)
            # s3 -> s3 must recompute the s3etag because the part layout may change
            checksums = dict(s3=checksum.S3EtagUnordered(), gs=checksum.GScrc32cUnordered())
        elif url.startswith("gs://"):
            bucket_name, key = url[5:].split("/", 1)
            gs_blob = GSBlob(bucket_name, key)
            parts = gs_blob.parts(threads)  # type: ignore
            checksums = dict(s3=checksum.S3EtagUnordered(), gs=gs_blob.cloud_native_checksum())
        else:
            parts = _file_part_iterator(url)  # type: ignore
            checksums = dict(s3=checksum.S3EtagUnordered(), gs=checksum.GScrc32cUnordered())
        key = f"{self.prefix}/{ssds_key}"
        with self.blobstore.blob(key).multipart_writer(threads) as uploader:
            for part in parts:
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

def _file_part_iterator(filepath: str) -> Generator[Part, None, None]:
    part_number = 0
    part_size = get_s3_multipart_chunk_size(os.path.getsize(filepath))
    with open(filepath, "rb") as fh:
        while True:
            data = fh.read(part_size)
            if not data:
                break
            yield Part(part_number, data)
            part_number += 1

def _list_tree(root) -> Generator[str, None, None]:
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)

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

    # TODO: pass in threads as optional parameter
    threads = 3
    with ThreadPoolExecutor(max_workers=threads) as e:
        oneshot_uploads = AsyncSet(e, threads)
        for key in src.blobstore.list(f"{src.prefix}/{submission_id}"):
            parts = src.blobstore.blob(key).parts(threads=threads)
            if 1 == len(parts):
                oneshot_uploads.put(_sync_oneshot, key, list(parts)[0].data)
            else:
                if not _already_synced(key):
                    logger.info(f"syncing {key} from {src} to {dst}")
                    with dst.blobstore.blob(key).multipart_writer(threads=threads) as writer:
                        for part in parts:
                            writer.put_part(part)
                    _verify_and_tag(key)
                    yield key
                else:
                    logger.info(f"already-synced {key} from {src} to {dst}")
            for synced_key in oneshot_uploads.consume_finished():
                yield synced_key
        for synced_key in oneshot_uploads.consume():
            if synced_key is not None:
                yield synced_key
