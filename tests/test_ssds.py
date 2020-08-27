#!/usr/bin/env python
import io
import os
import sys
import time
import logging
import unittest
import tempfile
from math import ceil
from uuid import uuid4
from random import randint
from typing import Iterable, Dict, Optional

from google.cloud import storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import ssds
from ssds.deployment import _S3StagingTest, _GSStagingTest
from ssds.blobstore import AWS_MIN_CHUNK_SIZE, AWS_MAX_MULTIPART_COUNT, MiB
from ssds.blobstore.s3 import S3BlobStore, S3AsyncPartIterator, get_s3_multipart_chunk_size
from ssds.blobstore.gs import GSBlobStore, GSAsyncPartIterator
from tests import infra, TestData


ssds.logger.level = logging.INFO
ssds.logger.addHandler(logging.StreamHandler(sys.stdout))

S3_SSDS = _S3StagingTest()
GS_SSDS = _GSStagingTest()

class TestSSDS(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_upload(self):
        with tempfile.TemporaryDirectory() as dirname:
            root = self._prepare_local_submission_dir(dirname)
            submission_name = "this_is_a_test_submission"
            tests = [
                ("aws sync", S3_SSDS, f"{uuid4()}", None),
                ("aws async", S3_SSDS, f"{uuid4()}", 4),
                ("gcp sync", GS_SSDS, f"{uuid4()}", None),
                ("gcp async", GS_SSDS, f"{uuid4()}", 4),
            ]
            for test_name, ds, submission_id, threads in tests:
                with self.subTest(test_name):
                    start_time = time.time()
                    for ssds_key in ds.upload(root, submission_id, submission_name, threads):
                        pass
                    print(f"{test_name} upload duration:", time.time() - start_time)
                    for filepath in ssds._list_tree(root):
                        expected_ssds_key = ds._compose_ssds_key(submission_id,
                                                                 submission_name,
                                                                 os.path.relpath(filepath, root))
                        key = f"{ds.prefix}/{expected_ssds_key}"
                        self.assertEqual(os.path.getsize(filepath), ds.blobstore.blob(key).size())

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

    def test_copy_local_to_cloud(self):
        submission_id = f"{uuid4()}"
        submission_name = "this_is_a_test_submission"
        expected_oneshot_data, expected_multipart_data = TestData.oneshot(), TestData.multipart()
        tests = [
            ("local to aws", S3_SSDS, f"{uuid4()}", expected_oneshot_data, None),
            ("local to aws", S3_SSDS, f"{uuid4()}", expected_multipart_data, None),
            ("local to aws", S3_SSDS, f"{uuid4()}", expected_multipart_data, 2),
            ("local to gcp", GS_SSDS, f"{uuid4()}", expected_oneshot_data, None),
            ("local to gcp", GS_SSDS, f"{uuid4()}", expected_multipart_data, None),
            ("local to gcp", GS_SSDS, f"{uuid4()}", expected_multipart_data, 2),
        ]
        for test_name, ds, submission_id, expected_data, threads in tests:
            with self.subTest(test_name, size=len(expected_data), threads=threads):
                submission_path = f"copy_{uuid4()}/fubar/snafu/file.biz"
                with tempfile.NamedTemporaryFile() as tf:
                    with open(tf.name, "wb") as fh:
                        fh.write(expected_data)
                    start_time = time.time()
                    ds.copy(tf.name, submission_id, submission_name, submission_path, threads)
                    print(f"{test_name} {len(expected_data)} bytes, threads={threads} upload duration:",
                          time.time() - start_time)
                    expected_ssds_key = ds._compose_ssds_key(submission_id,
                                                             submission_name,
                                                             submission_path)
                    key = f"{ds.prefix}/{expected_ssds_key}"
                    self.assertEqual(os.path.getsize(tf.name), ds.blobstore.blob(key).size())

    def test_copy_cloud_to_cloud(self):
        submission_id = f"{uuid4()}"
        submission_name = "this_is_a_test_submission"
        oneshot, multipart = TestData.uploaded([S3_SSDS.blobstore, GS_SSDS.blobstore])
        tests = [
            ("gcp to gcp", GS_SSDS, GS_SSDS, oneshot['key'], oneshot['data'], None),
            ("gcp to gcp", GS_SSDS, GS_SSDS, multipart['key'], multipart['data'], 4),

            ("aws to aws", S3_SSDS, S3_SSDS, oneshot['key'], oneshot['data'], None),
            ("aws to aws", S3_SSDS, S3_SSDS, multipart['key'], multipart['data'], 4),

            ("aws to gcp", S3_SSDS, GS_SSDS, oneshot['key'], oneshot['data'], None),
            ("aws to gcp", S3_SSDS, GS_SSDS, multipart['key'], multipart['data'], 4),

            ("gcp to aws", GS_SSDS, S3_SSDS, oneshot['key'], oneshot['data'], None),
            ("gcp to aws", GS_SSDS, S3_SSDS, multipart['key'], multipart['data'], 4),
        ]
        for test_name, src_ds, dst_ds, src_key, expected_data, threads in tests:
            with self.subTest(test_name, size=len(expected_data), threads=threads):
                src_url = f"{src_ds.blobstore.schema}{src_ds.bucket}/{src_key}"
                submission_path = f"copy_{uuid4()}/foo/bar/file.biz"
                start_time = time.time()
                dst_ds.copy(src_url, submission_id, submission_name, submission_path, threads)
                print(f"{test_name} {len(expected_data)} bytes, threads={threads} upload duration:",
                      time.time() - start_time)
                expected_ssds_key = dst_ds._compose_ssds_key(submission_id, submission_name, submission_path)
                dst_key = f"{dst_ds.prefix}/{expected_ssds_key}"
                self.assertEqual(src_ds.blobstore.blob(src_key).size(),
                                 dst_ds.blobstore.blob(dst_key).size())

    def test_sync(self):
        tests = [
            ("aws -> gcp", S3_SSDS, GS_SSDS),
            ("gcp -> aws", GS_SSDS, S3_SSDS)
        ]
        for test_name, src, dst in tests:
            with self.subTest(test_name):
                with tempfile.TemporaryDirectory() as dirname:
                    root = self._prepare_local_submission_dir(dirname)
                    submission_id = f"{uuid4()}"
                    submission_name = "this_is_a_test_submission_for_sync"
                    uploaded_keys = [ssds_key for ssds_key in src.upload(root,
                                                                         submission_id,
                                                                         submission_name,
                                                                         threads=4)]
                synced_keys = [key[len(f"{src.prefix}/"):] for key in ssds.sync(submission_id, src, dst)]
                dst_listed_keys = [ssds_key for ssds_key in dst.list_submission(submission_id)]
                self.assertEqual(sorted(uploaded_keys), sorted(synced_keys))
                self.assertEqual(sorted(uploaded_keys), sorted(dst_listed_keys))
                for ssds_key in dst_listed_keys:
                    a = src.blobstore.blob(f"{S3_SSDS.prefix}/{ssds_key}").get()
                    b = dst.blobstore.blob(f"{GS_SSDS.prefix}/{ssds_key}").get()
                    self.assertEqual(a, b)
                with self.subTest("test no resync"):
                    synced_keys = [key for key in ssds.sync(submission_id, src, dst) if key]
                    self.assertEqual(synced_keys, list())

    def _prepare_local_submission_dir(self, dirname: str, single_file=False) -> str:
        root = os.path.join(dirname, "test_submission")
        os.mkdir(root)
        if single_file:
            with open(os.path.join(root, "file.dat"), "wb") as fh:
                fh.write(TestData.oneshot())
        else:
            subdir1 = os.path.join(root, "subdir1")
            subdir2 = os.path.join(root, "subdir2")
            subsubdir = os.path.join(subdir1, "subsubdir")
            os.mkdir(subdir1)
            os.mkdir(subdir2)
            os.mkdir(subsubdir)
            for i in range(2):
                with open(os.path.join(root, f"zero_byte_file{i}.dat"), "wb") as fh:
                    fh.write(b"")
                with open(os.path.join(root, f"file{i}.dat"), "wb") as fh:
                    fh.write(TestData.oneshot())
                with open(os.path.join(subdir1, f"file{i}.dat"), "wb") as fh:
                    fh.write(TestData.oneshot())
                with open(os.path.join(subdir2, f"file{i}.dat"), "wb") as fh:
                    fh.write(TestData.oneshot())
                with open(os.path.join(subsubdir, f"file{i}.dat"), "wb") as fh:
                    fh.write(TestData.oneshot())
            with open(os.path.join(root, "large.dat"), "wb") as fh:
                fh.write(TestData.multipart())
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
        data = TestData.oneshot()
        blob = storage.Client().bucket(GS_SSDS.bucket).blob("test")
        with io.BytesIO(data) as fh:
            blob.upload_from_file(fh)
        blob.reload()
        cs = ssds.checksum.crc32c(data).google_storage_crc32c()
        self.assertEqual(blob.crc32c, cs)

    def test_blob_md5(self):
        data = TestData.oneshot()
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
            expected_data = TestData.oneshot()
            self._put_s3_obj(S3_SSDS.bucket, key, expected_data)
            data = S3BlobStore(S3_SSDS.bucket).blob(key).get()
            self.assertEqual(data, expected_data)
        with self.subTest("gcp"):
            key = f"{uuid4()}"
            expected_data = TestData.oneshot()
            self._put_gs_obj(S3_SSDS.bucket, key, expected_data)
            data = GSBlobStore(GS_SSDS.bucket).blob(key).get()
            self.assertEqual(data, expected_data)

    def _test_put(self):
        """
        This is implicitly tested during `TestSSDS`.
        """

    def test_size(self):
        tests = [("aws", S3_SSDS.bucket, S3_SSDS.blobstore, self._put_s3_obj),
                 ("gcp", GS_SSDS.bucket, GS_SSDS.blobstore, self._put_gs_obj)]
        key = f"{uuid4()}"
        for test_name, bucket, bs, upload in tests:
            expected_size = randint(1, 10)
            upload(bucket, key, os.urandom(expected_size))
            self.assertEqual(expected_size, bs.blob(key).size())

    def test_cloud_native_checksums(self):
        key = f"{uuid4()}"
        with self.subTest("aws"):
            blob = self._put_s3_obj(S3_SSDS.bucket, key, os.urandom(1))
            expected_checksum = blob.e_tag.strip("\"")
            self.assertEqual(expected_checksum, S3BlobStore(S3_SSDS.bucket).blob(key).cloud_native_checksum())
        with self.subTest("gcp"):
            blob = self._put_gs_obj(GS_SSDS.bucket, key, os.urandom(1))
            blob.reload()
            expected_checksum = blob.crc32c
            self.assertEqual(expected_checksum, GSBlobStore(GS_SSDS.bucket).blob(key).cloud_native_checksum())

    def test_part_iterators(self):
        _, multipart = TestData.uploaded([S3_SSDS.blobstore, GS_SSDS.blobstore])
        chunk_size = get_s3_multipart_chunk_size(len(multipart['data']))
        number_of_parts = ceil(len(multipart['data']) / chunk_size)
        expected_parts = [multipart['data'][i * chunk_size:(i + 1) * chunk_size]
                          for i in range(number_of_parts)]
        tests = [("aws", S3_SSDS.bucket, S3AsyncPartIterator),
                 ("gcp", GS_SSDS.bucket, GSAsyncPartIterator)]
        for replica_name, bucket_name, part_iterator in tests:
            with self.subTest(replica_name):
                count = 0
                for part_number, data in part_iterator(bucket_name, multipart['key'], threads=1):
                    self.assertEqual(expected_parts[part_number], data)
                    count += 1
                self.assertEqual(number_of_parts, count)

    def test_tags(self):
        key = f"{uuid4()}"
        tests = [("aws", S3_SSDS.blobstore, S3_SSDS.bucket, self._put_s3_obj),
                 ("gcp", GS_SSDS.blobstore, GS_SSDS.bucket, self._put_gs_obj)]
        for replica_name, blobstore, bucket_name, upload in tests:
            upload(bucket_name, key, b"")
            tags = dict(foo="bar", doom="gloom")
            blobstore.blob(key).put_tags(tags)
            self.assertEqual(tags, blobstore.blob(key).get_tags())

    def test_exists(self):
        key = f"{uuid4()}"
        tests = [("aws", S3_SSDS.blobstore, S3_SSDS.bucket, self._put_s3_obj),
                 ("gcp", GS_SSDS.blobstore, GS_SSDS.bucket, self._put_gs_obj)]
        for replica_name, blobstore, bucket_name, upload in tests:
            with self.subTest(replica=replica_name):
                self.assertFalse(blobstore.blob(key).exists())
                upload(bucket_name, key, b"")
                self.assertTrue(blobstore.blob(key).exists())

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
        expected_data = TestData.multipart()
        for threads in [None, 2]:
            with self.subTest(threads=threads):
                chunk_size = get_s3_multipart_chunk_size(len(expected_data))
                number_of_chunks = ceil(len(expected_data) / chunk_size)
                key = f"{uuid4()}"
                start_time = time.time()
                with S3MultipartWriter(S3_SSDS.bucket, key, threads) as writer:
                    for i in range(number_of_chunks):
                        part = Part(number=i,
                                    data=expected_data[i * chunk_size: (i + 1) * chunk_size])
                        writer.put_part(part)
                print(f"upload duration threads={threads}", time.time() - start_time)
                retrieved_data = ssds.aws.resource("s3").Bucket(S3_SSDS.bucket).Object(key).get()['Body'].read()
                self.assertEqual(expected_data, retrieved_data)

if __name__ == '__main__':
    unittest.main()
