#!/usr/bin/env python
import io
import os
import sys
import unittest
import tempfile
from uuid import uuid4
from random import randint

import boto3
from google.cloud import storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import ssds
from ssds import checksum, aws, s3, gs
from ssds.config import Staging, Platform
from tests import infra


_s3_staging_bucket = infra.get_env("SSDS_S3_STAGING_TEST_BUCKET")
_gs_staging_bucket = infra.get_env("SSDS_GS_STAGING_TEST_BUCKET")
_s3_release_bucket = infra.get_env("SSDS_S3_RELEASE_TEST_BUCKET")
_gs_release_bucket = infra.get_env("SSDS_GS_RELEASE_TEST_BUCKET")


class TestSSDS(unittest.TestCase, infra.SuppressWarningsMixin):
    def test_upload(self):
        with tempfile.TemporaryDirectory() as dirname:
            root = os.path.join(dirname, "test_submission")
            subdir1 = os.path.join(root, "subdir1")
            subdir2 = os.path.join(root, "subdir2")
            subsubdir = os.path.join(subdir1, "subsubdir")
            os.mkdir(root)
            os.mkdir(subdir1)
            os.mkdir(subdir2)
            os.mkdir(subsubdir)
            for i in range(2):
                with open(os.path.join(root, f"file{i}.dat"), "wb") as fh:
                    fh.write(os.urandom(200))
                with open(os.path.join(subdir1, f"file{i}.dat"), "wb") as fh:
                    fh.write(os.urandom(200))
                with open(os.path.join(subdir2, f"file{i}.dat"), "wb") as fh:
                    fh.write(os.urandom(200))
                with open(os.path.join(subsubdir, f"file{i}.dat"), "wb") as fh:
                    fh.write(os.urandom(200))
            with open(os.path.join(root, "large.dat"), "wb") as fh:
                fh.write(os.urandom(1024 ** 2 * 80))
            with self.subTest("AWS"):
                with Staging.override(Platform.aws, s3, _s3_staging_bucket):
                    ssds.upload(root, f"{uuid4()}", "this_is_a_test_submission")
            with self.subTest("GCP"):
                with Staging.override(Platform.gcp, gs, _gs_staging_bucket):
                    ssds.upload(root, f"{uuid4()}", "this_is_a_test_submission")


class TestSSDSChecksum(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_crc32c(self):
        data = b"\x89\xc0\xc6\xcd\xa9$=\xfa\x91\x86\xedi\xec\x18\xcc\xad\xd1\xe1\x82\x8f^\xd2\xdd$\x1fE\x821"
        expected_crc32c = "25c7a879"
        with self.subTest("all at once"):
            cs = checksum.crc32c(data)
            self.assertEqual(expected_crc32c, cs.hexdigest())
        with self.subTest("sliced"):
            i = randint(1, len(data) - 2)
            cs = checksum.crc32c(data[:i])
            cs.update(data[i:])
            self.assertEqual(expected_crc32c, cs.hexdigest())

    def test_blob_crc32c(self):
        data = os.urandom(200)
        blob = storage.Client().bucket(_gs_staging_bucket).blob("test")
        with io.BytesIO(data) as fh:
            blob.upload_from_file(fh)
        blob.reload()
        cs = checksum.crc32c(data).google_storage_crc32c()
        self.assertEqual(blob.crc32c, cs)

    def test_blob_md5(self):
        data = os.urandom(200)
        blob = aws.resource("s3").Bucket(_s3_staging_bucket).Object("test")
        with io.BytesIO(data) as fh:
            blob.upload_fileobj(fh)
        cs = checksum.md5(data).hexdigest()
        self.assertEqual(blob.e_tag.replace('"', ''), cs)

class TestS3Multipart(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_get_s3_multipart_chunk_size(self):
        from ssds.s3 import AWS_MIN_CHUNK_SIZE, AWS_MAX_MULTIPART_COUNT, MiB, get_s3_multipart_chunk_size
        with self.subTest("file size smaller than AWS_MAX_MULTIPART_COUNT * AWS_MIN_CHUNK_SIZE"):
            sz = AWS_MIN_CHUNK_SIZE * 2.234
            self.assertEqual(AWS_MIN_CHUNK_SIZE, get_s3_multipart_chunk_size(sz))
        with self.subTest("file size larger than AWS_MAX_MULTIPART_COUNT * AWS_MIN_CHUNK_SIZE"):
            base = AWS_MAX_MULTIPART_COUNT * AWS_MIN_CHUNK_SIZE
            pairs = [(base - 1, AWS_MIN_CHUNK_SIZE),
                     (base, AWS_MIN_CHUNK_SIZE),
                     (base + 1, AWS_MIN_CHUNK_SIZE + MiB),
                     (base + 10000 * MiB - 1, AWS_MIN_CHUNK_SIZE + MiB),
                     (base + 10000 * MiB, AWS_MIN_CHUNK_SIZE + MiB),
                     (base + 10000 * MiB + 1, AWS_MIN_CHUNK_SIZE + 2 * MiB)]
            for sz, expected_chunk_size in pairs:
                chunk_size = get_s3_multipart_chunk_size(sz)
                self.assertEqual(expected_chunk_size, chunk_size)


if __name__ == '__main__':
    unittest.main()
