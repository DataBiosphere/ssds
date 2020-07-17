import os
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, Optional, Generator

from ssds.blobstore import BlobStore
from ssds.blobstore.s3 import S3BlobStore


MAX_KEY_LENGTH = 1024  # this is the maximum length for S3 and GS object names
# GS docs: https://cloud.google.com/storage/docs/naming-objects
# S3 docs: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

class SSDS:
    blobstore: Optional[BlobStore] = None
    bucket: Optional[str] = None
    prefix = "submissions"
    _name_delimeter = "--"  # Not using "/" as name delimeter produces friendlier `aws s3` listing

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

    def get_submission_name(self, submission_id: str) -> str:
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

    def _upload_local_tree(self, root: str, submission_id: str, name: str) -> Generator[str, None, None]:
        root = os.path.normpath(root)
        assert root == os.path.abspath(root)
        assert " " not in name  # TODO: create regex to enforce name format?
        assert self._name_delimeter not in name  # TODO: create regex to enforce name format?
        filepaths = [p for p in _list_tree(root)]
        dst_prefix = f"{submission_id}{self._name_delimeter}{name}"
        ssds_keys = [f"{dst_prefix}/{os.path.relpath(p, root)}" for p in filepaths]
        for ssds_key in ssds_keys:
            key = f"{self.prefix}{ssds_key}"
            if MAX_KEY_LENGTH <= len(key):
                raise ValueError(f"Total key length must not exceed {MAX_KEY_LENGTH} characters {os.linesep}"
                                 f"{key} is too long {os.linesep}"
                                 f"Use a shorter submission name")
        for filepath, ssds_key in zip(filepaths, ssds_keys):
            dst_key = f"{self.prefix}/{ssds_key}"
            self.blobstore.upload_object(filepath, self.bucket, dst_key)
            yield ssds_key

    def compose_blobstore_url(self, ssds_key: str) -> str:
        return f"{self.blobstore.schema}{self.bucket}/{self.prefix}/{ssds_key}"

class Staging(SSDS):
    blobstore: Optional[BlobStore] = S3BlobStore()
    bucket = "human-pangenomics"

class Release(SSDS):
    def upload(self, *args, **kargs):
        raise NotImplementedError("Direct uploads to the release area are not supported.")

def _list_tree(root) -> Generator[str, None, None]:
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)

def sync(submission_id: str, src: SSDS, dst: SSDS):
    with ThreadPoolExecutor(max_workers=4) as e:
        for key in src.blobstore.list(src.bucket, f"{src.prefix}/{submission_id}"):
            parts = src.blobstore.parts(src.bucket, key, executor=e)
            if 1 == len(parts):
                dst.blobstore.put(dst.bucket, key, list(parts)[0].data)
                continue
            else:
                with dst.blobstore.multipart_writer(dst.bucket, key, e) as writer:
                    for part in parts:
                        writer.put_part(part)
