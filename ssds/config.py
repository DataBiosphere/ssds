import typing
from enum import Enum


class Platform(Enum):
    AWS = "AWS"
    GCP = "GCP"

class Config:
    platform: typing.Optional[Platform] = None
    staging_bucket: typing.Optional[str] = None
    release_bucket: typing.Optional[str] = None
    gcp_project: typing.Optional[str] = None

    @classmethod
    def set(cls, platform: Platform, staging_bucket: str, release_bucket: str):
        assert platform in Platform
        cls.platform = platform
        cls.staging_bucket = staging_bucket
        cls.release_bucket = release_bucket
