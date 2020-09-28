#!/usr/bin/env python
import io
import os
import sys
import time
import logging
import tempfile
import unittest
from unittest import mock
from uuid import uuid4
from random import randint
from collections import defaultdict
from typing import Tuple, Optional

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from ssds import SSDSObjectTag, checksum
from ssds import storage
from ssds.blobstore.s3 import S3BlobStore, S3Blob
from ssds.blobstore.gs import GSBlobStore, GSBlob
from ssds.blobstore.local import LocalBlobStore, LocalBlob
from ssds.deployment import _S3StagingTest, _GSStagingTest
from tests import infra, TestData


s3_test_bucket = _S3StagingTest.bucket
s3_blobstore = S3BlobStore(s3_test_bucket)
gs_test_bucket = _GSStagingTest.bucket
gs_blobstore = GSBlobStore(gs_test_bucket)
local_test_tempdir = tempfile.TemporaryDirectory()
local_test_bucket = local_test_tempdir.name
local_blobstore = LocalBlobStore(local_test_tempdir.name)
test_data = TestData()

logging.basicConfig(stream=sys.stderr, level=logging.INFO)
storage.logger.setLevel(logging.INFO)

class TestStorage(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_copy(self):
        src_blob = mock.MagicMock()
        dst_blob = mock.MagicMock()
        with mock.patch("ssds.storage.CopyClient.copy") as copy_method:
            storage.copy(src_blob, dst_blob)
            copy_method.assert_called_once()

    def test_copy_client(self):
        oneshot, multipart = test_data.uploaded([local_blobstore, s3_blobstore, gs_blobstore])
        expected_data_map = dict()

        with storage.CopyClient(ignore_missing_checksums=True) as client:
            for src_bs in (local_blobstore, s3_blobstore, gs_blobstore):
                for dst_bs in (local_blobstore, s3_blobstore, gs_blobstore):
                    for data_bundle in (oneshot, multipart):
                        src_blob = src_bs.blob(data_bundle['key'])
                        dst_blob = dst_bs.blob(f"{uuid4()}")
                        client.copy(src_blob, dst_blob)
                        expected_data_map[dst_blob] = data_bundle['data']
        for blob, expected_data in expected_data_map.items():
            with self.subTest(blob.url):
                self.assertEqual(blob.get(), expected_data)

        with self.subTest("test copy errors"):
            with self.assertRaises(storage.SSDSCopyError):
                with storage.CopyClient() as client:
                    for dst_bs in (s3_blobstore, gs_blobstore):
                        for src_key in (oneshot['key'], multipart['key']):
                            src_blob = src_bs.blob(src_key)
                            dst_blob = dst_bs.blob(f"{uuid4()}")
                            with self.assertRaises(storage.SSDSCopyError):
                                client.copy(src_blob, dst_blob)

    def test_verify_checksums(self):
        tests = [(S3Blob, SSDSObjectTag.SSDS_MD5),
                 (GSBlob, SSDSObjectTag.SSDS_CRC32C)]
        for blob_class, tag_key in tests:
            with self.subTest(blob_class=blob_class):
                checksums = {tag_key: f"{uuid4()}"}

                dst_blob = mock.MagicMock(spec=blob_class)
                with self.assertRaises(storage.SSDSIncorrectChecksum):
                    storage.verify_checksums("test", dst_blob, checksums)

                dst_blob.cloud_native_checksum = mock.MagicMock(return_value=checksums[tag_key])
                storage.verify_checksums("test", dst_blob, checksums)

                dst_blob = mock.MagicMock(spec=blob_class)
                checksums = dict()
                with self.assertRaises(storage.SSDSMissingChecksum):
                    storage.verify_checksums("test", dst_blob, checksums)

                dst_blob = mock.MagicMock(spec=blob_class)
                checksums = dict()
                storage.verify_checksums("test", dst_blob, checksums, ignore_missing_checksums=True)

        dst_blob = mock.MagicMock(spec=LocalBlob)
        storage.verify_checksums("test", dst_blob, checksums)

if __name__ == '__main__':
    unittest.main()
