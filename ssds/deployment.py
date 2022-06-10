from enum import Enum
from functools import lru_cache

from ssds import SSDS

from ssds.blobstore.s3 import S3BlobStore
from ssds.blobstore.gs import GSBlobStore


class _S3Staging(SSDS):
    blobstore_class = S3BlobStore
    bucket = "human-pangenomics"

class _GSStaging(SSDS):
    blobstore_class = GSBlobStore
    # Bucket associated with workspace `AnVIL_HPRC`
    bucket = "fc-4310e737-a388-4a10-8c9e-babe06aaf0cf"

class _S3StagingTest(SSDS):
    blobstore_class = S3BlobStore
    bucket = "org-hpp-ssds-staging-test-platform-dev"

class _GSStagingTest(SSDS):
    blobstore_class = GSBlobStore
    bucket = "org-hpp-ssds-staging-test"

class _GSStagingTestTNU(SSDS):
    blobstore_class = GSBlobStore
    bucket = "fc-9169fcd1-92ce-4d60-9d2d-d19fd326ff10"

class _Release(SSDS):
    def upload(self, *args, **kargs):
        raise NotImplementedError("Direct uploads to the release area are not supported.")

class _S3ReleaseTest(SSDS):
    blobstore_class = S3BlobStore
    bucket = "org-hpp-ssds-release-test"

class _GSReleaseTest(SSDS):
    blobstore_class = GSBlobStore
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
