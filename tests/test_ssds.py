#!/usr/bin/env python
import io
import os
import sys
import time
import logging
import unittest
import tempfile
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import ssds
import ssds.blobstore
from ssds.blobstore.local import LocalBlob, LocalBlobStore
from ssds.deployment import _S3StagingTest, _GSStagingTest
from tests import infra, TestData


ssds.logger.level = logging.INFO
ssds.logger.addHandler(logging.StreamHandler(sys.stdout))

S3_SSDS = _S3StagingTest()
GS_SSDS = _GSStagingTest()

class TestSSDS(infra.SuppressWarningsMixin, unittest.TestCase):
    test_data = TestData(1, 1)  # appease mypy

    @classmethod
    def setUpClass(cls):
        cls._old_aws_min_chunk_size = ssds.blobstore.AWS_MIN_CHUNK_SIZE
        ssds.blobstore.AWS_MIN_CHUNK_SIZE = 1024 * 1024 * 5
        cls.test_data = TestData(7, ssds.blobstore.AWS_MIN_CHUNK_SIZE + 1)

    @classmethod
    def tearDownClass(cls):
        ssds.blobstore.AWS_MIN_CHUNK_SIZE = cls._old_aws_min_chunk_size

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
                    for blob in LocalBlobStore(root).list():
                        expected_ssds_key = ds._compose_ssds_key(submission_id,
                                                                 submission_name,
                                                                 os.path.relpath(blob.key, root))
                        dst_key = f"{ds.prefix}/{expected_ssds_key}"
                        self.assertEqual(LocalBlob(blob.key).size(), ds.blobstore.blob(dst_key).size())

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
        expected_oneshot_data, expected_multipart_data = self.test_data.oneshot, self.test_data.multipart
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
        oneshot, multipart = self.test_data.uploaded([S3_SSDS.blobstore, GS_SSDS.blobstore])
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
                fh.write(self.test_data.oneshot)
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
                    fh.write(self.test_data.oneshot)
                with open(os.path.join(subdir1, f"file{i}.dat"), "wb") as fh:
                    fh.write(self.test_data.oneshot)
                with open(os.path.join(subdir2, f"file{i}.dat"), "wb") as fh:
                    fh.write(self.test_data.oneshot)
                with open(os.path.join(subsubdir, f"file{i}.dat"), "wb") as fh:
                    fh.write(self.test_data.oneshot)
            with open(os.path.join(root, "large.dat"), "wb") as fh:
                fh.write(self.test_data.multipart)
        return root

if __name__ == '__main__':
    unittest.main()
