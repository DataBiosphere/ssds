#!/usr/bin/env python
import io
import os
import sys
import unittest
import tempfile
from uuid import uuid4
from random import randint
from math import ceil

from google.cloud import storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import ssds
from ssds.blobstore.s3 import S3BlobStore, S3AsyncPartIterator, get_s3_multipart_chunk_size
from ssds.blobstore.gs import GSBlobStore, GSAsyncPartIterator
from tests import infra


_s3_staging_bucket = infra.get_env("SSDS_S3_STAGING_TEST_BUCKET")
_gs_staging_bucket = infra.get_env("SSDS_GS_STAGING_TEST_BUCKET")
_s3_release_bucket = infra.get_env("SSDS_S3_RELEASE_TEST_BUCKET")
_gs_release_bucket = infra.get_env("SSDS_GS_RELEASE_TEST_BUCKET")

# Prevent accidental data upload to main HPP bucket
ssds.Staging.blobstore = None
ssds.Staging.bucket = None

class _StagingS3(ssds.Staging):
    blobstore = S3BlobStore()
    bucket = _s3_staging_bucket
StagingS3 = _StagingS3()

class _StagingGS(ssds.Staging):
    blobstore = GSBlobStore()
    bucket = _gs_staging_bucket
StagingGS = _StagingGS()

class TestSSDS(infra.SuppressWarningsMixin, unittest.TestCase):
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
                fh.write(os.urandom(1024 ** 2 * 160))
            submission_id = f"{uuid4()}"
            submission_name = "this_is_a_test_submission"
            with self.subTest("aws"):
                for ssds_key in StagingS3.upload(root, submission_id, submission_name):
                    print(StagingS3.compose_blobstore_url(ssds_key))
            with self.subTest("gcp"):
                for ssds_key in StagingGS.upload(root, submission_id, submission_name):
                    print(StagingGS.compose_blobstore_url(ssds_key))

    def test_upload_name_length_error(self):
        with tempfile.TemporaryDirectory() as dirname:
            root = os.path.join(dirname, "test_submission")
            os.mkdir(root)
            with open(os.path.join(root, "file.dat"), "wb") as fh:
                fh.write(os.urandom(200))
            submission_id = f"{uuid4()}"
            submission_name = "a" * ssds.MAX_KEY_LENGTH
            with self.subTest("aws"):
                with self.assertRaises(ValueError):
                    for _ in StagingS3.upload(root, submission_id, submission_name):
                        pass
            with self.subTest("gcp"):
                with self.assertRaises(ValueError):
                    for _ in StagingGS.upload(root, submission_id, submission_name):
                        pass

    def test_upload_name_collisions(self):
        with tempfile.TemporaryDirectory() as dirname:
            root = os.path.join(dirname, "test_submission")
            os.mkdir(root)
            with open(os.path.join(root, "file.dat"), "wb") as fh:
                fh.write(os.urandom(200))
            submission_id = f"{uuid4()}"
            submission_name = None
            with self.subTest("Must provide name for new submission"):
                with self.assertRaises(ValueError):
                    for _ in StagingS3.upload(root, submission_id, submission_name):
                        pass
            with self.subTest("Should succeed with a name"):
                submission_name = "name_collision_test_submission"
                for ssds_key in StagingS3.upload(root, submission_id, submission_name):
                    print(StagingS3.compose_blobstore_url(ssds_key))
            with self.subTest("Should raise if provided name collides with existing name"):
                submission_name = "name_collision_test_submission_wrong_name"
                with self.assertRaises(ValueError):
                    for ssds_key in StagingS3.upload(root, submission_id, submission_name):
                        pass
            with self.subTest("Submitting submission again should succeed while omitting name"):
                for ssds_key in StagingS3.upload(root, submission_id):
                    print(StagingS3.compose_blobstore_url(ssds_key))


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
        data = os.urandom(200)
        blob = storage.Client().bucket(_gs_staging_bucket).blob("test")
        with io.BytesIO(data) as fh:
            blob.upload_from_file(fh)
        blob.reload()
        cs = ssds.checksum.crc32c(data).google_storage_crc32c()
        self.assertEqual(blob.crc32c, cs)

    def test_blob_md5(self):
        data = os.urandom(200)
        blob = ssds.aws.resource("s3").Bucket(_s3_staging_bucket).Object("test")
        with io.BytesIO(data) as fh:
            blob.upload_fileobj(fh)
        cs = ssds.checksum.md5(data).hexdigest()
        self.assertEqual(blob.e_tag.replace('"', ''), cs)

class TestS3Multipart(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_get_s3_multipart_chunk_size(self):
        from ssds.blobstore import AWS_MIN_CHUNK_SIZE, AWS_MAX_MULTIPART_COUNT, MiB
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

class TestBlobStore(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_schema(self):
        self.assertEqual("s3://", S3BlobStore.schema)
        self.assertEqual("gs://", GSBlobStore.schema)

    def test_get(self):
        with self.subTest("aws"):
            key = f"{uuid4()}"
            expected_data = os.urandom(10)
            self._put_s3_obj(_s3_staging_bucket, key, expected_data)
            data = S3BlobStore().get(_s3_staging_bucket, key)
            self.assertEqual(data, expected_data)
        with self.subTest("gcp"):
            key = f"{uuid4()}"
            expected_data = os.urandom(10)
            self._put_gs_obj(_s3_staging_bucket, key, expected_data)
            data = GSBlobStore().get(_gs_staging_bucket, key)
            self.assertEqual(data, expected_data)

    def _test_put(self):
        """
        This is implicitly tested during `TestSSDS`.
        """

    def test_cloud_native_checksums(self):
        key = f"{uuid4()}"
        with self.subTest("aws"):
            blob = self._put_s3_obj(_s3_staging_bucket, key, os.urandom(1))
            expected_checksum = blob.e_tag.strip("\"")
            self.assertEqual(expected_checksum, S3BlobStore().cloud_native_checksum(_s3_staging_bucket, key))
        with self.subTest("gcp"):
            blob = self._put_gs_obj(_gs_staging_bucket, key, os.urandom(1))
            blob.reload()
            expected_checksum = blob.crc32c
            self.assertEqual(expected_checksum, GSBlobStore().cloud_native_checksum(_gs_staging_bucket, key))

    def test_part_iterators(self):
        key = f"{uuid4()}"
        expected_data = os.urandom(1024 * 1024 * 130)
        chunk_size = get_s3_multipart_chunk_size(len(expected_data))
        number_of_parts = ceil(len(expected_data) / chunk_size)
        expected_parts = [expected_data[i * chunk_size:(i + 1) * chunk_size]
                          for i in range(number_of_parts)]
        tests = [("aws", _s3_staging_bucket, self._put_s3_obj, S3AsyncPartIterator),
                 ("gcp", _gs_staging_bucket, self._put_gs_obj, GSAsyncPartIterator)]
        for replica, bucket_name, uploade, part_iterator in tests:
            with self.subTest(replica):
                uploade(bucket_name, key, expected_data)
                count = 0
                for part_number, data in part_iterator(bucket_name, key):
                    self.assertEqual(expected_parts[part_number], data)
                    count += 1
                self.assertEqual(number_of_parts, count)

    def _put_s3_obj(self, bucket, key, data):
        blob = ssds.aws.resource("s3").Bucket(bucket).Object(key)
        blob.upload_fileobj(io.BytesIO(data))
        return blob

    def _put_gs_obj(self, bucket, key, data):
        from ssds.blobstore import gs
        blob = gs._client().bucket(bucket).blob(key)
        blob.upload_from_file(io.BytesIO(data))
        return blob

if __name__ == '__main__':
    unittest.main()
