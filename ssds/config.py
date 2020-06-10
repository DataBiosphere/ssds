import types
import typing
from enum import Enum

from ssds import s3, gs


class Platform(Enum):
    aws = "aws"
    gcp = "gcp"

class _Config:
    platform: typing.Optional[Platform] = None
    blobstore: typing.Optional[types.ModuleType] = None
    bucket: typing.Optional[str] = None

    @classmethod
    def override(cls, platform=None, blobstore=None, bucket=None):
        class _ConfigOverride:
            def __enter__(self, *args, **kwargs):
                self._old_platform = cls.platform
                self._old_blobstore = cls.blobstore
                self._old_bucket = cls.bucket
                cls.platform = platform or cls.platform
                cls.blobstore = blobstore or cls.blobstore
                cls.bucket = bucket or cls.bucket

            def __exit__(self, *args, **kwargs):
                cls.platform = self._old_platform
                cls.blobstore = self._old_blobstore
                cls.bucket = self._old_bucket

        return _ConfigOverride()

class Staging(_Config):
    platform = Platform.aws
    blobstore = s3
    bucket = "org-hpp-ssds-staging-test"

class Release(_Config):
    pass
