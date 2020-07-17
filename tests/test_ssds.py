#!/usr/bin/env python
import io
import os
import sys
import unittest
import tempfile
from math import ceil
from uuid import uuid4
from random import randint
from concurrent.futures import ThreadPoolExecutor

from google.cloud import storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import ssds
from ssds.deployment import _S3StagingTest, _GSStagingTest
from ssds.blobstore import AWS_MIN_CHUNK_SIZE, AWS_MAX_MULTIPART_COUNT, MiB
from ssds.blobstore.s3 import S3BlobStore, S3AsyncPartIterator, get_s3_multipart_chunk_size
from ssds.blobstore.gs import GSBlobStore, GSAsyncPartIterator
from tests import infra


S3_SSDS = _S3StagingTest()
GS_SSDS = _GSStagingTest()

class TestSSDS(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_upload(self):
        with tempfile.TemporaryDirectory() as dirname:
            root = self._prepare_local_submission_dir(dirname)
            submission_id = f"{uuid4()}"
            submission_name = "this_is_a_test_submission"
            with self.subTest("aws"):
                for ssds_key in S3_SSDS.upload(root, submission_id, submission_name):
                    print(S3_SSDS.compose_blobstore_url(ssds_key))
            with self.subTest("gcp"):
                for ssds_key in GS_SSDS.upload(root, submission_id, submission_name):
                    print(GS_SSDS.compose_blobstore_url(ssds_key))

    def test_upload_name_length_error(self):
        with tempfile.TemporaryDirectory() as dirname:
            root = self._prepare_local_submission_dir(dirname, single_file=True)
            submission_id = f"{uuid4()}"
            submission_name = "a" * ssds.MAX_KEY_LENGTH
            with self.subTest("aws"):
                with self.assertRaises(ValueError):
                    for _ in S3_SSDS.upload(root, submission_id, submission_name):
                        pass
            with self.subTest("gcp"):
                with self.assertRaises(ValueError):
                    for _ in GS_SSDS.upload(root, submission_id, submission_name):
                        pass

    def test_upload_name_collisions(self):
        with tempfile.TemporaryDirectory() as dirname:
            root = self._prepare_local_submission_dir(dirname, single_file=True)
            submission_id = f"{uuid4()}"
            submission_name = None
            with self.subTest("Must provide name for new submission"):
                with self.assertRaises(ValueError):
                    for _ in S3_SSDS.upload(root, submission_id, submission_name):
                        pass
            with self.subTest("Should succeed with a name"):
                submission_name = "name_collision_test_submission"
                for ssds_key in S3_SSDS.upload(root, submission_id, submission_name):
                    print(S3_SSDS.compose_blobstore_url(ssds_key))
            with self.subTest("Should raise if provided name collides with existing name"):
                submission_name = "name_collision_test_submission_wrong_name"
                with self.assertRaises(ValueError):
                    for ssds_key in S3_SSDS.upload(root, submission_id, submission_name):
                        pass
            with self.subTest("Submitting submission again should succeed while omitting name"):
                for ssds_key in S3_SSDS.upload(root, submission_id):
                    print(S3_SSDS.compose_blobstore_url(ssds_key))

    def test_sync(self):
        tests = [("aws -> gcp", S3_SSDS, GS_SSDS),
                 ("gcp -> aws", GS_SSDS, S3_SSDS)]
        for test_name, src, dst in tests:
            with self.subTest(test_name):
                with tempfile.TemporaryDirectory() as dirname:
                    root = self._prepare_local_submission_dir(dirname)
                    submission_id = f"{uuid4()}"
                    submission_name = "this_is_a_test_submission_for_sync"
                    uploaded_keys = [ssds_key for ssds_key in src.upload(root, submission_id, submission_name)]
                for key in ssds.sync(submission_id, src, dst):
                    pass
                synced_keys = [ssds_key for ssds_key in dst.list_submission(submission_id)]
                self.assertEqual(sorted(uploaded_keys), sorted(synced_keys))
                for ssds_key in synced_keys:
                    a = src.blobstore.get(S3_SSDS.bucket, f"{S3_SSDS.prefix}/{ssds_key}")
                    b = dst.blobstore.get(GS_SSDS.bucket, f"{GS_SSDS.prefix}/{ssds_key}")
                    self.assertEqual(a, b)

    def _prepare_local_submission_dir(self, dirname: str, single_file=False) -> str:
        root = os.path.join(dirname, "test_submission")
        os.mkdir(root)
        if single_file:
            with open(os.path.join(root, "file.dat"), "wb") as fh:
                fh.write(os.urandom(200))
        else:
            subdir1 = os.path.join(root, "subdir1")
            subdir2 = os.path.join(root, "subdir2")
            subsubdir = os.path.join(subdir1, "subsubdir")
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
        return root

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
        blob = storage.Client().bucket(GS_SSDS.bucket).blob("test")
        with io.BytesIO(data) as fh:
            blob.upload_from_file(fh)
        blob.reload()
        cs = ssds.checksum.crc32c(data).google_storage_crc32c()
        self.assertEqual(blob.crc32c, cs)

    def test_blob_md5(self):
        data = os.urandom(200)
        blob = ssds.aws.resource("s3").Bucket(S3_SSDS.bucket).Object("test")
        with io.BytesIO(data) as fh:
            blob.upload_fileobj(fh)
        cs = ssds.checksum.md5(data).hexdigest()
        self.assertEqual(blob.e_tag.replace('"', ''), cs)

class TestS3Multipart(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_get_s3_multipart_chunk_size(self):
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
            self._put_s3_obj(S3_SSDS.bucket, key, expected_data)
            data = S3BlobStore().get(S3_SSDS.bucket, key)
            self.assertEqual(data, expected_data)
        with self.subTest("gcp"):
            key = f"{uuid4()}"
            expected_data = os.urandom(10)
            self._put_gs_obj(S3_SSDS.bucket, key, expected_data)
            data = GSBlobStore().get(GS_SSDS.bucket, key)
            self.assertEqual(data, expected_data)

    def _test_put(self):
        """
        This is implicitly tested during `TestSSDS`.
        """

    def test_cloud_native_checksums(self):
        key = f"{uuid4()}"
        with self.subTest("aws"):
            blob = self._put_s3_obj(S3_SSDS.bucket, key, os.urandom(1))
            expected_checksum = blob.e_tag.strip("\"")
            self.assertEqual(expected_checksum, S3BlobStore().cloud_native_checksum(S3_SSDS.bucket, key))
        with self.subTest("gcp"):
            blob = self._put_gs_obj(GS_SSDS.bucket, key, os.urandom(1))
            blob.reload()
            expected_checksum = blob.crc32c
            self.assertEqual(expected_checksum, GSBlobStore().cloud_native_checksum(GS_SSDS.bucket, key))

    def test_part_iterators(self):
        key = f"{uuid4()}"
        expected_data = os.urandom(1024 * 1024 * 130)
        chunk_size = get_s3_multipart_chunk_size(len(expected_data))
        number_of_parts = ceil(len(expected_data) / chunk_size)
        expected_parts = [expected_data[i * chunk_size:(i + 1) * chunk_size]
                          for i in range(number_of_parts)]
        tests = [("aws", S3_SSDS.bucket, self._put_s3_obj, S3AsyncPartIterator),
                 ("gcp", GS_SSDS.bucket, self._put_gs_obj, GSAsyncPartIterator)]
        for replica_name, bucket_name, upload, part_iterator in tests:
            with self.subTest(replica_name):
                upload(bucket_name, key, expected_data)
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

    def test_s3_multipart_writer(self):
        from ssds.blobstore.s3 import S3MultipartWriter, Part
        import time
        workers = S3MultipartWriter.concurrent_uploads
        tests = [("synchronous", None), ("asynchronous", ThreadPoolExecutor(max_workers=workers))]
        expected_data = os.urandom(1024**2 * 130)
        for test_name, executor in tests:
            with self.subTest(test_name):
                chunk_size = get_s3_multipart_chunk_size(len(expected_data))
                number_of_chunks = ceil(len(expected_data) / chunk_size)
                key = f"{uuid4()}"
                start_time = time.time()
                with S3MultipartWriter(S3_SSDS.bucket, key, executor) as writer:
                    for i in range(number_of_chunks):
                        part = Part(number=i,
                                    data=expected_data[i * chunk_size: (i + 1) * chunk_size])
                        writer.put_part(part)
                if executor:
                    executor.shutdown()
                print("Multipart upload duration", test_name, time.time() - start_time)
                retrieved_data = ssds.aws.resource("s3").Bucket(S3_SSDS.bucket).Object(key).get()['Body'].read()
                self.assertEqual(expected_data, retrieved_data)

if __name__ == '__main__':
    unittest.main()
