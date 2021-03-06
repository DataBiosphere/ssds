#!/usr/bin/env python
import io
import os
import sys
import unittest
from random import randint, shuffle

from google.cloud import storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import ssds
from ssds.deployment import _S3StagingTest, _GSStagingTest
from tests import infra, TestData


s3_test_bucket = _S3StagingTest.bucket
gs_test_bucket = _GSStagingTest.bucket

test_data = TestData()

class TestSSDSChecksum(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_crc32c(self):
        data = b"\x89\xc0\xc6\xcd\xa9$=\xfa\x91\x86\xedi\xec\x18\xcc\xad\xd1\xe1\x82\x8f^\xd2\xdd$\x1fE\x821"
        expected_crc32c = "25c7a879"
        with self.subTest("all at once"):
            cs = ssds.checksum.crc32c(data)
            self.assertEqual(expected_crc32c, cs.hexdigest())
        with self.subTest("sliced"):
            i = randint(1, len(data) - 2)
            cs = ssds.checksum.crc32c(data[:i])
            cs.update(data[i:])
            self.assertEqual(expected_crc32c, cs.hexdigest())

    def test_blob_crc32c(self):
        data = test_data.oneshot
        blob = storage.Client().bucket(gs_test_bucket).blob("test")
        with io.BytesIO(data) as fh:
            blob.upload_from_file(fh)
        blob.reload()
        cs = ssds.checksum.crc32c(data).google_storage_crc32c()
        self.assertEqual(blob.crc32c, cs)

    def test_blob_md5(self):
        data = test_data.oneshot
        blob = ssds.aws.resource("s3").Bucket(s3_test_bucket).Object("test")
        with io.BytesIO(data) as fh:
            blob.upload_fileobj(fh)
        cs = ssds.checksum.md5(data).hexdigest()
        self.assertEqual(blob.e_tag.replace('"', ''), cs)

    def test_s3etag_unordered(self):
        checksums = set()
        chunks = [(i, os.urandom(10)) for i in range(20)]
        expected_checksum = ssds.checksum.compute_composite_etag(
            [ssds.checksum.md5(c[1]).hexdigest() for c in chunks]
        )
        for _ in range(10):
            shuffle(chunks)
            cs = ssds.checksum.S3EtagUnordered()
            for chunk_number, chunk in chunks:
                cs.update(chunk_number, chunk)
            checksums.add(cs.hexdigest())
        self.assertEqual(1, len(checksums))
        self.assertEqual(expected_checksum, checksums.pop())

    def test_gscrc32c_unordered(self):
        checksums = set()
        chunks = [(i, os.urandom(10)) for i in range(5)]
        expected_checksum = ssds.checksum.crc32c(b"".join([c[1] for c in chunks])).google_storage_crc32c()
        for _ in range(10):
            shuffle(chunks)
            cs = ssds.checksum.GScrc32cUnordered()
            for chunk_number, chunk in chunks:
                cs.update(chunk_number, chunk)
            checksums.add(cs.hexdigest())
        self.assertEqual(1, len(checksums))
        self.assertEqual(expected_checksum, checksums.pop())

if __name__ == '__main__':
    unittest.main()
