from enum import Enum
from functools import lru_cache

from ssds import SSDS

from ssds.blobstore.s3 import S3BlobStore
from ssds.blobstore.gs import GSBlobStore


class _S3Staging(SSDS):
    blobstore = S3BlobStore()
    bucket = "human-pangenomics"

class _GSStaging(SSDS):
    blobstore = GSBlobStore()
    # Bucket associated with workspace `AnVIL_HPRC`
    bucket = "fc-4310e737-a388-4a10-8c9e-babe06aaf0cf"

class _S3StagingTest(SSDS):
    blobstore = S3BlobStore()
    bucket = "org-hpp-ssds-staging-test"

class _GSStagingTest(SSDS):
    blobstore = GSBlobStore()
    bucket = "org-hpp-ssds-staging-test"

class _Release(SSDS):
    def upload(self, *args, **kargs):
        raise NotImplementedError("Direct uploads to the release area are not supported.")

class _S3ReleaseTest(SSDS):
    blobstore = S3BlobStore()
    bucket = "org-hpp-ssds-release-test"

class _GSReleaseTest(SSDS):
    blobstore = GSBlobStore()
    bucket = "org-hpp-ssds-release-test"

class Staging(Enum):
    default = _S3Staging
    gcp = _GSStaging
    aws_test = _S3StagingTest
    gcp_test = _GSStagingTest
    # Release = _Release

    @property
    def ssds(self) -> SSDS:
        return self.value()

class Release(Enum):
    aws_test = _S3ReleaseTest
    gcp_test = _GSReleaseTest

    @property
    def ssds(self) -> SSDS:
        return self.value()
