import os
import typing

from ssds.blobstore import BlobStore
from ssds.blobstore.s3 import S3BlobStore


MAX_KEY_LENGTH = 1024  # this is the maximum length for S3 and GS object names
# GS docs: https://cloud.google.com/storage/docs/naming-objects
# S3 docs: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

class SSDS:
    blobstore: typing.Optional[BlobStore] = None
    bucket: typing.Optional[str] = None
    prefix: typing.Optional[str] = None
    _name_delimeter = "--"  # Not using "/" as name delimeter produces friendlier `aws s3` listing

    @classmethod
    def list(cls):
        listing = cls.blobstore.list(cls.bucket, cls.prefix)
        prev_submission_id = ""
        for key in listing:
            try:
                ssds_key = key.strip(f"{cls.prefix}/")
                submission_id, parts = ssds_key.split(cls._name_delimeter, 1)
                submission_name, _ = parts.split("/", 1)
            except ValueError:
                continue
            if submission_id != prev_submission_id:
                yield submission_id, submission_name
                prev_submission_id = submission_id

    @classmethod
    def list_submission(cls, submission_id: str):
        for key in cls.blobstore.list(cls.bucket, f"{cls.prefix}/{submission_id}"):
            ssds_key = key.strip(f"{cls.prefix}/")
            yield ssds_key

    @classmethod
    def get_submission_name(cls, submission_id: str):
        name = None
        for key in cls.blobstore.list(cls.bucket, f"{cls.prefix}/{submission_id}"):
            ssds_key = key.strip(f"{cls.prefix}/")
            _, parts = ssds_key.split(cls._name_delimeter, 1)
            name, _ = parts.split("/", 1)
            break
        return name

    @classmethod
    def upload(cls,
               root: str,
               submission_id: str,
               name: typing.Optional[str]=None) -> typing.Generator[str, None, None]:
        """
        Upload files from root directory and yield ssds_key for each file.
        This returns a generator that must be iterated for uploads to occur.
        """
        existing_name = cls.get_submission_name(submission_id)
        if not name:
            if not existing_name:
                raise ValueError("Must provide name for new submissions")
            name = existing_name
        elif existing_name and existing_name != name:
            raise ValueError("Cannot update name of existing submission")
        for ssds_key in cls._upload_local_tree(root, submission_id, name):
            yield ssds_key

    @classmethod
    def _upload_local_tree(cls, root: str, submission_id: str, name: str):
        root = os.path.normpath(root)
        assert root == os.path.abspath(root)
        assert " " not in name  # TODO: create regex to enforce name format?
        assert cls._name_delimeter not in name  # TODO: create regex to enforce name format?
        filepaths = [p for p in _list_tree(root)]
        dst_prefix = f"{submission_id}{cls._name_delimeter}{name}"
        ssds_keys = [f"{dst_prefix}/{os.path.relpath(p, root)}" for p in filepaths]
        for ssds_key in ssds_keys:
            key = f"{cls.prefix}{ssds_key}"
            if MAX_KEY_LENGTH <= len(key):
                raise ValueError(f"Total key length must not exceed {MAX_KEY_LENGTH} characters {os.linesep}"
                                 f"{key} is too long {os.linesep}"
                                 f"Use a shorter submission name")
        for filepath, ssds_key in zip(filepaths, ssds_keys):
            dst_key = f"{cls.prefix}/{ssds_key}"
            cls.blobstore.upload_object(filepath, cls.bucket, dst_key)
            yield ssds_key

    @classmethod
    def compose_blobstore_url(cls, ssds_key: str):
        return f"{cls.blobstore.schema}{cls.bucket}/{cls.prefix}/{ssds_key}"

    @classmethod
    def override(cls, blobstore=None, bucket=None, prefix=None):
        """
        Context manager for temporarily changing configuration
        """
        class _ConfigOverride:
            def __enter__(self, *args, **kwargs):
                self._old_blobstore = cls.blobstore
                self._old_bucket = cls.bucket
                self._old_prefix = cls.prefix
                cls.blobstore = blobstore or cls.blobstore
                cls.bucket = bucket or cls.bucket
                cls.prefix = prefix or cls.prefix

            def __exit__(self, *args, **kwargs):
                cls.blobstore = self._old_blobstore
                cls.bucket = self._old_bucket
                cls.prefix = self._old_prefix

        return _ConfigOverride()

class Staging(SSDS):
    blobstore = S3BlobStore()
    bucket = "human-pangenomics"
    prefix = "submissions"

class Release(SSDS):
    @classmethod
    def upload(cls, *args, **kargs):
        raise NotImplementedError("Direct uploads to the release area are not supported.")

def _list_tree(root):
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)
