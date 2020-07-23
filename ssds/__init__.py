import os
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Tuple, Optional, Generator

from ssds import checksum
from ssds.blobstore import BlobStore, get_s3_multipart_chunk_size, Part
from ssds.blobstore.s3 import S3BlobStore


MAX_KEY_LENGTH = 1024  # this is the maximum length for S3 and GS object names
# GS docs: https://cloud.google.com/storage/docs/naming-objects
# S3 docs: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

class SSDSObjectTag:
    SSDS_MD5 = "SSDS_MD5"
    SSDS_CRC32C = "SSDS_CRC32C"

class SSDS:
    blobstore_class = BlobStore
    bucket = str()
    prefix = "submissions"
    _name_delimeter = "--"  # Not using "/" as name delimeter produces friendlier `aws s3` listing

    def __init__(self):
        self.blobstore = self.blobstore_class()

    def list(self) -> Generator[Tuple[str, str], None, None]:
        listing = self.blobstore.list(self.bucket, self.prefix)
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

    def list_submission(self, submission_id: str) -> Generator[str, None, None]:
        for key in self.blobstore.list(self.bucket, f"{self.prefix}/{submission_id}"):
            ssds_key = key.replace(f"{self.prefix}/", "", 1)
            yield ssds_key

    def get_submission_name(self, submission_id: str) -> Optional[str]:
        name = None
        for key in self.blobstore.list(self.bucket, f"{self.prefix}/{submission_id}"):
            ssds_key = key.strip(f"{self.prefix}/")
            _, parts = ssds_key.split(self._name_delimeter, 1)
            name, _ = parts.split("/", 1)
            break
        return name

    def upload(self,
               root: str,
               submission_id: str,
               name: Optional[str]=None) -> Generator[str, None, None]:
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
        for ssds_key in self._upload_local_tree(root, submission_id, name):
            yield ssds_key

    def _upload_local_tree(self,
                           root: str,
                           submission_id: str,
                           name: str) -> Generator[str, None, None]:
        root = os.path.normpath(root)
        assert root == os.path.abspath(root)
        assert " " not in name  # TODO: create regex to enforce name format?
        assert self._name_delimeter not in name  # TODO: create regex to enforce name format?

        for filepath in _list_tree(root):
            ssds_key = self._compose_ssds_key(submission_id, name, os.path.relpath(filepath, root))
            size = os.stat(filepath).st_size
            part_size = get_s3_multipart_chunk_size(size)
            if part_size >= size:
                yield self._upload_oneshot(filepath, ssds_key)
            else:
                yield self._upload_multipart(filepath, ssds_key, part_size)

    def _compose_ssds_key(self, submission_id: str, submission_name: str, path: str) -> str:
        dst_prefix = f"{submission_id}{self._name_delimeter}{submission_name}"
        ssds_key = f"{dst_prefix}/{path}"
        blobstore_key = f"{self.prefix}{ssds_key}"
        if MAX_KEY_LENGTH <= len(blobstore_key):
            raise ValueError(f"Total key length must not exceed {MAX_KEY_LENGTH} characters {os.linesep}"
                             f"{blobstore_key} is too long {os.linesep}"
                             f"Use a shorter submission name")
        return ssds_key

    def _upload_oneshot(self, filepath: str, ssds_key: str) -> str:
        key = f"{self.prefix}/{ssds_key}"
        with open(filepath, "rb") as fh:
            data = fh.read()
        gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
        s3_etag = checksum.md5(data).hexdigest()
        self.blobstore.put(self.bucket, key, data)
        tags = {SSDSObjectTag.SSDS_MD5: s3_etag, SSDSObjectTag.SSDS_CRC32C: gs_crc32c}
        self.blobstore.put_tags(self.bucket, key, tags)
        return ssds_key

    def _upload_multipart(self, filepath: str, ssds_key: str, part_size: int) -> str:
        key = f"{self.prefix}/{ssds_key}"
        s3_etags = list()
        crc32c = checksum.crc32c(b"")
        with self.blobstore.multipart_writer(self.bucket, key) as uploader:
            part_number = 0
            with open(filepath, "rb") as fh:
                while True:
                    data = fh.read(part_size)
                    if not data:
                        break
                    s3_etags.append(checksum.md5(data).hexdigest())
                    crc32c.update(data)
                    uploader.put_part(Part(part_number, data))
                    part_number += 1

        s3_etag = checksum.compute_composite_etag(s3_etags)
        gs_crc32c = crc32c.google_storage_crc32c()
        tags = {SSDSObjectTag.SSDS_MD5: s3_etag, SSDSObjectTag.SSDS_CRC32C: gs_crc32c}
        self.blobstore.put_tags(self.bucket, key, tags)
        return ssds_key

    def compose_blobstore_url(self, ssds_key: str) -> str:
        return f"{self.blobstore.schema}{self.bucket}/{self.prefix}/{ssds_key}"

def _list_tree(root) -> Generator[str, None, None]:
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)

def sync(submission_id: str, src: SSDS, dst: SSDS) -> Generator[str, None, None]:
    def _verify_and_tag(key: str):
        src_tags = src.blobstore.get_tags(src.bucket, key)
        dst_checksum = dst.blobstore.cloud_native_checksum(dst.bucket, key)
        if "gs://" == dst.blobstore.schema:
            assert src_tags[SSDSObjectTag.SSDS_CRC32C] == dst_checksum
        elif "s3://" == dst.blobstore.schema:
            assert src_tags[SSDSObjectTag.SSDS_MD5] == dst_checksum
        else:
            raise RuntimeError("Unknown blobstore schema!")
        dst.blobstore.put_tags(dst.bucket, key, src_tags)

    def _sync_oneshot(key: str, data: bytes):
        dst.blobstore.put(dst.bucket, key, data)
        _verify_and_tag(key)

    with ThreadPoolExecutor(max_workers=4) as e:
        for key in src.blobstore.list(src.bucket, f"{src.prefix}/{submission_id}"):
            yield key
            parts = src.blobstore.parts(src.bucket, key, executor=e)
            if 1 == len(parts):
                f = e.submit(_sync_oneshot, key, list(parts)[0].data)
                f.add_done_callback(lambda f: f.result())  # raise exceptions encountered during future execution
            else:
                with dst.blobstore.multipart_writer(dst.bucket, key, executor=e) as writer:
                    for part in parts:
                        writer.put_part(part)
                _verify_and_tag(key)
