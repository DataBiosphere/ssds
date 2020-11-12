#!/usr/bin/env python
import io
import os
import sys
import time
import gzip
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

from ssds import storage, checksum
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
        with self.subTest("should work"):
            expected_data_map, completed_keys = self._do_blobstore_copies()
            self.assertEqual(len(expected_data_map), len(completed_keys))
            for blob, expected_data in expected_data_map.items():
                with self.subTest(blob.url):
                    self.assertEqual(blob.get(), expected_data)

    def test_copy_client_gzip(self):
        """
        Under somewhat mysterious circumstances, S3 computes the Etag of gzipped objects using uncompressed contents.
        This test verifies Etags are computed using binary data of source file (uncompressed contents).
        """
        src = self._get_problem_gzip_blob()
        dst = s3_blobstore.blob(f"{uuid4()}")
        with storage.CopyClient() as client:
            client.copy_compute_checksums(src, dst)

    def _get_problem_gzip_blob(self) -> S3Blob:
        """
        Grab a gzip file that has caused Etag errors in the past.
        Cache it in S3 to avoid frequent downloads from NIH servers.
        """
        problem_gzip_blob = s3_blobstore.blob("gzip.fixture.gz")
        if not problem_gzip_blob.exists():
            from ftplib import FTP
            ftp = FTP("ftp-trace.ncbi.nlm.nih.gov")
            ftp.login(user="", passwd="")
            ftp.cwd("ReferenceSamples/giab/data/ChineseTrio/HG006_NA24694-huCA017E_father/NA24694_Father_HiSeq100x/"
                    "NA24694_Father_HiSeq100x_fastqs/141020_D00360_0062_AHB657ADXX/Sample_NA24694")
            filename = "NA24694_GCCAAT_L002_R1_039.fastq.gz"
            with io.BytesIO() as raw:
                ftp.retrbinary("RETR " + filename, raw.write)
                problem_gzip_blob.put(raw.getvalue())
        return problem_gzip_blob

    def test_copy_client_compute_checksums(self):
        expected_data_map, completed_keys = self._do_blobstore_copies((local_blobstore, s3_blobstore, gs_blobstore),
                                                                      (s3_blobstore, gs_blobstore),
                                                                      compute_checksums=True)
        self.assertEqual(len(expected_data_map), len(completed_keys))
        for blob, expected_data in expected_data_map.items():
            with self.subTest(blob.url):
                self.assertEqual(blob.get(), expected_data)

    def _do_blobstore_copies(self,
                             src_blobstores=(local_blobstore, s3_blobstore, gs_blobstore),
                             dst_blobstores=(local_blobstore, s3_blobstore, gs_blobstore),
                             ignore_missing_checksums=True,
                             compute_checksums=False):
        oneshot, _ = test_data.uploaded(src_blobstores)
        expected_data_map = dict()
        with storage.CopyClient(ignore_missing_checksums=ignore_missing_checksums) as client:
            for src_bs in src_blobstores:
                for dst_bs in dst_blobstores:
                    for data_bundle in (oneshot,):
                        src_blob = src_bs.blob(data_bundle['key'])
                        dst_blob = dst_bs.blob(os.path.join(f"{uuid4()}", f"{uuid4()}"))
                        if compute_checksums:
                            client.copy_compute_checksums(src_blob, dst_blob)
                        else:
                            client.copy(src_blob, dst_blob)
                        expected_data_map[dst_blob] = data_bundle['data']
        return expected_data_map, [dst_blob.key for src_blob, dst_blob, exc in client.completed()
                                   if exc is None]

    def test_verify_checksums(self):
        for blob_class, tag_key in [(S3Blob, storage.SSDSObjectTag.SSDS_MD5),
                                    (GSBlob, storage.SSDSObjectTag.SSDS_CRC32C)]:
            dst_blob = mock.MagicMock(spec=blob_class)
            tests = [({tag_key: "correct"}, "correct", storage.SSDSChecksumStatus.ok),
                     ({tag_key: "wrong"}, "not-right!", storage.SSDSChecksumStatus.incorrect),
                     ({}, "missing", storage.SSDSChecksumStatus.missing)]
            for src_tags, dst_native_checksum, expected_status in tests:
                with self.subTest(blob_class=blob_class, src_tags=src_tags, expected_status=expected_status):
                    dst_blob.cloud_native_checksum = mock.MagicMock(return_value=dst_native_checksum)
                    status, cs_tag = storage.verify_checksums(dst_blob, src_tags)
                    self.assertEqual(expected_status, status)
                    if status != storage.SSDSChecksumStatus.ok:
                        self.assertEqual(cs_tag, tag_key)

        with self.subTest(blob_class=LocalBlob):
            dst_blob = mock.MagicMock(spec=LocalBlob)
            with self.assertRaises(KeyError):
                storage.verify_checksums(dst_blob, {})

    def test_transform_key(self):
        src_key = "some/key/or/other/to/what.txt"
        src_pfx = "/some/key/"
        dst_pfx = "bro/what/george////"
        dst_key = storage.transform_key(src_key, src_pfx, dst_pfx)
        self.assertEqual("bro/what/george/or/other/to/what.txt", dst_key)

    def test_parse_cloud_url(self):
        url = "gs://gobble/axe"
        self.assertEqual(("gobble", "axe"), storage.parse_cloud_url(url))

        url = "s3://turkey/dinner/with/stuffing"
        self.assertEqual(("turkey", "dinner/with/stuffing"), storage.parse_cloud_url(url))

        url = "this isn't valid"
        with self.assertRaises(ValueError):
            storage.parse_cloud_url(url)

if __name__ == '__main__':
    unittest.main()
