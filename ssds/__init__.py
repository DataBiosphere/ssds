import os
import types
import typing

from ssds import s3, gs


MAX_KEY_LENGTH = 1024  # this is the maximum length for S3 and GS object names
# GS docs: https://cloud.google.com/storage/docs/naming-objects
# S3 docs: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

class SSDS:
    blobstore: typing.Optional[types.ModuleType] = None
    bucket: typing.Optional[str] = None
    prefix: typing.Optional[str] = None

    @classmethod
    def list(cls):
        listing = cls.blobstore.list(cls.bucket, cls.prefix)
        prev_submission_id = ""
        for key in listing:
            try:
                submission_id, parts = key.split("--", 1)
                submission_name, _ = parts.split("/", 1)
            except ValueError:
                continue
            if submission_id != prev_submission_id:
                yield submission_id, submission_name
                prev_submission_id = submission_id

    @classmethod
    def list_submission(cls, submission_id: str):
        for key in cls.blobstore.list(cls.bucket, f"{cls.prefix}/{submission_id}"):  # type: ignore
            yield key

    @classmethod
    def upload(cls, src: str, submission_id: str, description: str):
        cls._upload_local_tree(src, submission_id, description)

    @classmethod
    def _upload_local_tree(cls, root: str, submission_id: str, description: str):
        root = os.path.normpath(root)
        assert root == os.path.abspath(root)
        assert " " not in description  # TODO: create regex to enforce description format?
        assert "--" not in description  # TODO: create regex to enforce description format?
        filepaths = [p for p in _list_tree(root)]
        dst_prefix = f"{cls.prefix}/{submission_id}--{description}"
        dst_keys = [f"{dst_prefix}/{os.path.relpath(p, root)}" for p in filepaths]
        for key in dst_keys:
            if MAX_KEY_LENGTH <= len(key):
                raise ValueError(f"Total key length must not exceed {MAX_KEY_LENGTH} characters {os.linesep}"
                                 f"{key} is too long {os.linesep}"
                                 f"Use a shorter submission name")
        for filepath, dst_key in zip(filepaths, dst_keys):
            print(f"Uploading {cls.compose_blobstore_url(dst_key)}")
            cls.blobstore.upload_object(filepath, cls.bucket, dst_key)  # type: ignore

    @classmethod
    def compose_blobstore_url(cls, key):
        return f"{cls.blobstore.schema}{cls.bucket}/{key}"

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
    blobstore = s3
    bucket = "org-hpp-ssds-staging-test"
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
