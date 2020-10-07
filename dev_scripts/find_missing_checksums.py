#!/usr/bin/env python
import os
import sys
import logging
from typing import Union

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from ssds import SSDSObjectTag
from ssds.deployment import _S3Staging, _GSStaging
from ssds.blobstore.s3 import S3Blob
from ssds.blobstore.gs import GSBlob
from ssds.concurrency import Executor, async_set


logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout)
logger.level = logging.INFO
Executor.max_workers = 10

def verify_tags(blob: Union[S3Blob, GSBlob]):
    tags = blob.get_tags()
    logger.info(f"checking: {blob.url}")
    for key in (SSDSObjectTag.SSDS_MD5, SSDSObjectTag.SSDS_CRC32C):
        if key not in tags:
            logger.warning(f"missing {SSDSObjectTag.SSDS_MD5}: {blob.url}")

verify_tag_operations = async_set()

for blob in _GSStaging.blobstore_class(_GSStaging.bucket).list("submissions"):
    verify_tag_operations.put(verify_tags, blob)

for blob in _S3Staging.blobstore_class(_S3Staging.bucket).list("submissions"):
    verify_tag_operations.put(verify_tags, blob)

for _ in verify_tag_operations.consume():
    pass
