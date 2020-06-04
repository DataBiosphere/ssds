from enum import Enum
import typing

gcp_project = "hpp-ucsc"
_aws_staging_bucket = "org-hpp-ssds-staging-test"
_gcp_staging_bucket = "org-hpp-ssds-staging-test"
_aws_release_bucket = "org-hpp-ssds-release-test"
_gcp_release_bucket = "org-hpp-ssds-release-test"

class Platform(Enum):
    AWS = "AWS"
    GCP = "GCP"

platform = Platform.AWS
staging_bucket = _aws_staging_bucket
release_bucket = _aws_release_bucket
