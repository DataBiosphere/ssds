#!/usr/bin/env python
import os
import sys
import time
import json
import logging
import unittest
import unittest.mock
import tempfile
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import ssds
import ssds.blobstore
from ssds.blobstore.s3 import S3BlobStore, S3Blob
from ssds.blobstore.gs import GSBlobStore
from ssds.blobstore.local import LocalBlob, LocalBlobStore
from ssds.deployment import _S3StagingTest, _GSStagingTest
from tests import infra
from tests.fixtures.populate import populate_fixtures


ssds.logger.level = logging.INFO
ssds.logger.addHandler(logging.StreamHandler(sys.stdout))

S3_SSDS = _S3StagingTest()
GS_SSDS = _GSStagingTest()

class TestSSDS(infra.SuppressWarningsMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._old_aws_min_chunk_size = ssds.blobstore.AWS_MIN_CHUNK_SIZE
        ssds.blobstore.AWS_MIN_CHUNK_SIZE = 1024 * 1024 * 5

        cls.tempdir = tempfile.TemporaryDirectory()
        cls.testdir = cls.tempdir.name
        cls.oneshot_data = os.urandom(7)
        cls.multipart_data = os.urandom(2 * ssds.blobstore.AWS_MIN_CHUNK_SIZE + 1)
        cls.submission_tree, cls.multifile_pfx = populate_fixtures(cls.testdir,
                                                                   cls.oneshot_data,
                                                                   cls.multipart_data)
        cls.singlefile_testdir = os.path.join(cls.testdir, "singelfile")
        for data_style, filepath, remote_key in cls.submission_tree:
            if "ONESHOT" == data_style:
                cls.oneshot_key = remote_key
            if "MULTIPART" == data_style:
                cls.multipart_key = remote_key

    @classmethod
    def tearDownClass(cls):
        ssds.blobstore.AWS_MIN_CHUNK_SIZE = cls._old_aws_min_chunk_size

    def test_upload(self):
        submission_name = "this_is_a_test_submission"
        tests = [
            ("local to aws", LocalBlobStore, self.testdir, "", S3_SSDS, f"{uuid4()}"),
            ("local to gcp", LocalBlobStore, self.testdir, "", GS_SSDS, f"{uuid4()}"),
            ("aws to aws", S3BlobStore, "org-hpp-ssds-upload-test", f"{self.multifile_pfx}", S3_SSDS, f"{uuid4()}"),
            ("aws to gcp", S3BlobStore, "org-hpp-ssds-upload-test", f"{self.multifile_pfx}", GS_SSDS, f"{uuid4()}"),
            ("gcp to aws", GSBlobStore, "org-hpp-ssds-upload-test", f"{self.multifile_pfx}", S3_SSDS, f"{uuid4()}"),
            ("gcp to gcp", GSBlobStore, "org-hpp-ssds-upload-test", f"{self.multifile_pfx}", GS_SSDS, f"{uuid4()}"),
        ]
        for subdir in ("", "alsdfjlasdjf/laksfkljasd", "/asfas/", "asdfsa///"):
            for test_name, src_blobstore, src_bucket, src_pfx, dst_ds, submission_id in tests:
                src_url = f"{src_blobstore.schema}{src_bucket}/{src_pfx}"
                with self.subTest(test_name, subdir=subdir):
                    start_time = time.time()
                    for ssds_key in dst_ds.upload(src_url, submission_id, submission_name, subdir):
                        pass
                    print(test_name, "took", time.time() - start_time, "seconds")

                    def verify_upload(src_blob):
                        suffix = ((subdir.strip("/") + "/" if subdir else "")
                                  + src_blob.key.replace(src_pfx, "", 1).strip("/"))
                        expected_ssds_key = f"{submission_id}{dst_ds._name_delimeter}{submission_name}/{suffix}"
                        dst_key = f"{dst_ds.prefix}/{expected_ssds_key}"
                        dst_blob = dst_ds.blobstore.blob(dst_key)
                        print("checking", src_blob.url, "->", dst_blob.url)
                        self.assertEqual(src_blob.size(), dst_blob.size())

                    with ThreadPoolExecutor(max_workers=8) as e:
                        e.map(verify_upload, [b for b in src_blobstore(src_bucket).list(src_pfx)])

    def test_upload_name_length_error(self):
        submission_id = f"{uuid4()}"
        submission_name = "a" * ssds.MAX_KEY_LENGTH
        with self.subTest("aws"):
            with self.assertRaises(ValueError):
                for _ in S3_SSDS.upload(self.testdir, submission_id, submission_name):
                    pass
        with self.subTest("gcp"):
            with self.assertRaises(ValueError):
                for _ in GS_SSDS.upload(self.testdir, submission_id, submission_name):
                    pass

    def test_upload_name_collisions(self):
        submission_id = f"{uuid4()}"
        submission_name = None
        with self.subTest("Must provide name for new submission"):
            with self.assertRaises(ValueError):
                for _ in S3_SSDS.upload(self.singlefile_testdir, submission_id, submission_name):
                    pass
        with self.subTest("Should succeed with a name"):
            submission_name = "name_collision_test_submission"
            for ssds_key in S3_SSDS.upload(self.singlefile_testdir, submission_id, submission_name):
                print(S3_SSDS.compose_blobstore_url(ssds_key))
        with self.subTest("Should raise if provided name collides with existing name"):
            submission_name = "name_collision_test_submission_wrong_name"
            with self.assertRaises(ValueError):
                for ssds_key in S3_SSDS.upload(self.singlefile_testdir, submission_id, submission_name):
                    pass
        with self.subTest("Submitting submission again should succeed while omitting name"):
            for ssds_key in S3_SSDS.upload(self.singlefile_testdir, submission_id):
                print(S3_SSDS.compose_blobstore_url(ssds_key))

    def test_copy_local_to_cloud(self):
        submission_id = f"{uuid4()}"
        submission_name = "this_is_a_test_submission"
        expected_oneshot_data, expected_multipart_data = self.oneshot_data, self.multipart_data
        tests = [
            ("local to aws", S3_SSDS, f"{uuid4()}", expected_oneshot_data),
            ("local to aws", S3_SSDS, f"{uuid4()}", expected_multipart_data),
            ("local to gcp", GS_SSDS, f"{uuid4()}", expected_oneshot_data),
            ("local to gcp", GS_SSDS, f"{uuid4()}", expected_multipart_data),
        ]
        for test_name, ds, submission_id, expected_data in tests:
            with self.subTest(test_name, size=len(expected_data)):
                submission_path = f"copy_{uuid4()}/fubar/snafu/file.biz"
                with tempfile.NamedTemporaryFile() as tf:
                    with open(tf.name, "wb") as fh:
                        fh.write(expected_data)
                    start_time = time.time()
                    ds.copy(tf.name, submission_id, submission_name, submission_path)
                    print(f"{test_name} {len(expected_data)} bytes, upload duration:",
                          time.time() - start_time)
                    expected_ssds_key = ds._compose_ssds_key(submission_id,
                                                             submission_name,
                                                             submission_path)
                    key = f"{ds.prefix}/{expected_ssds_key}"
                    self.assertEqual(os.path.getsize(tf.name), ds.blobstore.blob(key).size())

    def test_copy_cloud_to_cloud(self):
        submission_id = f"{uuid4()}"
        submission_name = "this_is_a_test_submission"
        tests = [
            ("gcp to gcp", "gs://", self.oneshot_key, GS_SSDS, self.oneshot_data),
            ("gcp to gcp", "gs://", self.multipart_key, GS_SSDS, self.multipart_data),

            ("aws to aws", "s3://", self.oneshot_key, S3_SSDS, self.oneshot_data),
            ("aws to aws", "s3://", self.multipart_key, S3_SSDS, self.multipart_data),

            ("aws to gcp", "s3://", self.oneshot_key, GS_SSDS, self.oneshot_data),
            ("aws to gcp", "s3://", self.multipart_key, GS_SSDS, self.multipart_data),

            ("gcp to aws", "gs://", self.oneshot_key, S3_SSDS, self.oneshot_data),
            ("gcp to aws", "gs://", self.multipart_key, S3_SSDS, self.multipart_data),
        ]
        for test_name, src_schema, src_key, dst_ds, expected_data in tests:
            with self.subTest(test_name, size=len(expected_data)):
                src_url = f"{src_schema}org-hpp-ssds-upload-test/{src_key}"
                submission_path = f"copy_{uuid4()}/foo/bar/file.biz"
                start_time = time.time()
                dst_ds.copy(src_url, submission_id, submission_name, submission_path)
                print(f"{test_name} {len(expected_data)} bytes, upload duration:",
                      time.time() - start_time)
                expected_ssds_key = dst_ds._compose_ssds_key(submission_id, submission_name, submission_path)
                dst_key = f"{dst_ds.prefix}/{expected_ssds_key}"
                self.assertEqual(len(expected_data), dst_ds.blobstore.blob(dst_key).size())

    def test_sync(self):
        tests = [
            ("aws -> gcp", S3_SSDS, GS_SSDS),
            ("gcp -> aws", GS_SSDS, S3_SSDS)
        ]
        for test_name, src, dst in tests:
            with self.subTest(test_name):
                submission_id = f"{uuid4()}"
                submission_name = "this_is_a_test_submission_for_sync"
                uploaded_keys = set([ssds_key for ssds_key in src.upload(self.testdir,
                                                                     submission_id,
                                                                     submission_name)])
                synced_keys = [key[len(f"{src.prefix}/"):] for key in ssds.sync(submission_id, src, dst)]
                self._assert_sync(src, dst, submission_id, uploaded_keys, synced_keys)

    def _assert_sync(self, src, dst, submission_id, expected, synced, already_synced=None, subdir=None):
        dst_listed_keys = {ssds_key for ssds_key in dst.list_submission(submission_id)
                           if subdir is None or subdir.strip('/') in ssds_key}
        self.assertEqual(sorted(expected), sorted(synced))
        self.assertTrue(expected.issubset(dst_listed_keys))
        self.assertTrue(dst_listed_keys.issubset(expected.union(already_synced or set())))
        for ssds_key in dst_listed_keys:
            a = src.blobstore.blob(f"{S3_SSDS.prefix}/{ssds_key}").get()
            b = dst.blobstore.blob(f"{GS_SSDS.prefix}/{ssds_key}").get()
            self.assertEqual(a, b)
        with self.subTest("test no resync"):
            synced = [key for key in ssds.sync(submission_id, src, dst, subdir=subdir) if key]
            self.assertEqual(synced, list())

    def test_sync_subdir(self):
        src = S3_SSDS
        dst = GS_SSDS
        submission_id = f"{uuid4()}"
        submission_name = "this_is_a_test_submission_for_sync"
        uploaded_keys = [ssds_key for ssds_key in src.upload(self.testdir,
                                                             submission_id,
                                                             submission_name)]
        already_synced = set()
        for subdir in ['subdir1/subsubdir', '/subdir1/', 'subdir2']:
            with self.subTest(subdir=subdir):
                expected_keys = set([k for k in uploaded_keys if subdir.strip('/') in k]) - already_synced
                synced_keys = [key[len(f"{src.prefix}/"):] for key in ssds.sync(submission_id, src, dst, subdir=subdir)]
                already_synced.update(synced_keys)
                self._assert_sync(src, dst, submission_id, expected_keys, synced_keys,
                                  already_synced=already_synced, subdir=subdir)

    def test_release(self):
        for src in (S3_SSDS, GS_SSDS):
            submission_id = f"{uuid4()}"
            submission_name = "this_is_a_test_submission_for_release"
            uploaded_keys = [ssds_key for ssds_key in src.upload(self.testdir,
                                                                 submission_id,
                                                                 submission_name)]
            fake_key = f"{submission_id}--{submission_name}/does-not-exit"
            uploaded_keys.extend([fake_key])
            for dst in (S3_SSDS, GS_SSDS):
                with self.subTest(src=src, dst=dst):
                    transfers = list()
                    for ssds_key in uploaded_keys:
                        src_url = src.compose_blobstore_url(ssds_key)
                        dst_url = f"{dst.blobstore.schema}{dst.bucket}/working/{uuid4()}"
                        transfers.append((src_url, dst_url))
                    with self.assertRaises(AssertionError):
                        manifest = ssds.release(f"this-does-not-exist-{uuid4()}", src, dst, transfers)
                    manifest = ssds.release(submission_id, src, dst, transfers)
                    ssds_key = src._compose_ssds_key(submission_id,
                                                     submission_name,
                                                     f"release-transfer-manifests/{manifest['start_timestamp']}")
                    key = f"{src.prefix}/{ssds_key}"
                    blob = src.blobstore.blob(key)
                    self.assertTrue(blob.exists())
                    self.assertEqual(manifest, json.loads(blob.get().decode("utf-8")))
                    sources = set(m['src_key'] for m in manifest['transfer_map'])
                    destinations = set(m['dst_key'] for m in manifest['transfer_map'])
                    for src_url, dst_url in transfers:
                        if fake_key not in src_url:
                            src_blob = ssds.storage.blob_for_url(src_url)
                            dst_blob = ssds.storage.blob_for_url(dst_url)
                            self.assertEqual(src_blob.get(), dst_blob.get())
                            self.assertIn(src_blob.key, sources)
                            self.assertIn(dst_blob.key, destinations)


class NoFixturesTests(unittest.TestCase):

    def test_get_full_prefix(self):
        with unittest.mock.patch.object(S3_SSDS.blobstore, 'list') as mock_list:
            mock_list.return_value.__next__.return_value = S3Blob(
                bucket_name='org-hpp-ssds-staging-test-platform-dev',
                key='submissions/dc4385e0-0553-4a8b-b000-9542b7d990c3--this_is_a_test_submission_for_sync/foo/bar/bert.dat'
            )
            full_prefix = S3_SSDS.get_submission_prefix('foo')
        expected = 'submissions/dc4385e0-0553-4a8b-b000-9542b7d990c3--this_is_a_test_submission_for_sync'
        self.assertEqual(expected, full_prefix)


if __name__ == '__main__':
    unittest.main()
