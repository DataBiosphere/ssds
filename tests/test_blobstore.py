#!/usr/bin/env python
import io
import os
import sys
import time
import unittest
from math import ceil
from uuid import uuid4
from random import randint

from google.cloud import storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from ssds import aws
from ssds.blobstore import AWS_MIN_CHUNK_SIZE, AWS_MAX_MULTIPART_COUNT, MiB, get_s3_multipart_chunk_size, Part
from ssds.blobstore.s3 import S3BlobStore, S3AsyncPartIterator, S3MultipartWriter
from ssds.blobstore.gs import GSBlobStore, GSAsyncPartIterator, _client
from ssds.deployment import _S3StagingTest, _GSStagingTest
from tests import infra, TestData


s3_test_bucket = _S3StagingTest.bucket
s3_blobstore = S3BlobStore(s3_test_bucket)

gs_test_bucket = _GSStagingTest.bucket
gs_blobstore = GSBlobStore(gs_test_bucket)

test_data = TestData()

class TestBlobStore(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_schema(self):
        self.assertEqual("s3://", S3BlobStore.schema)
        self.assertEqual("gs://", GSBlobStore.schema)

    def test_get(self):
        with self.subTest("aws"):
            key = f"{uuid4()}"
            expected_data = test_data.oneshot
            self._put_s3_obj(s3_test_bucket, key, expected_data)
            data = S3BlobStore(s3_test_bucket).blob(key).get()
            self.assertEqual(data, expected_data)
        with self.subTest("gcp"):
            key = f"{uuid4()}"
            expected_data = test_data.oneshot
            self._put_gs_obj(s3_test_bucket, key, expected_data)
            data = GSBlobStore(gs_test_bucket).blob(key).get()
            self.assertEqual(data, expected_data)

    def _test_put(self):
        """
        This is implicitly tested during `TestSSDS`.
        """

    def test_size(self):
        tests = [("aws", s3_test_bucket, s3_blobstore, self._put_s3_obj),
                 ("gcp", gs_test_bucket, gs_blobstore, self._put_gs_obj)]
        key = f"{uuid4()}"
        for test_name, bucket, bs, upload in tests:
            expected_size = randint(1, 10)
            upload(bucket, key, os.urandom(expected_size))
            self.assertEqual(expected_size, bs.blob(key).size())

    def test_cloud_native_checksums(self):
        key = f"{uuid4()}"
        with self.subTest("aws"):
            blob = self._put_s3_obj(s3_test_bucket, key, os.urandom(1))
            expected_checksum = blob.e_tag.strip("\"")
            self.assertEqual(expected_checksum, S3BlobStore(s3_test_bucket).blob(key).cloud_native_checksum())
        with self.subTest("gcp"):
            blob = self._put_gs_obj(gs_test_bucket, key, os.urandom(1))
            blob.reload()
            expected_checksum = blob.crc32c
            self.assertEqual(expected_checksum, GSBlobStore(gs_test_bucket).blob(key).cloud_native_checksum())

    def test_part_iterators(self):
        _, multipart = test_data.uploaded([s3_blobstore, gs_blobstore])
        chunk_size = get_s3_multipart_chunk_size(len(multipart['data']))
        number_of_parts = ceil(len(multipart['data']) / chunk_size)
        expected_parts = [multipart['data'][i * chunk_size:(i + 1) * chunk_size]
                          for i in range(number_of_parts)]
        tests = [("aws", s3_test_bucket, S3AsyncPartIterator),
                 ("gcp", gs_test_bucket, GSAsyncPartIterator)]
        for replica_name, bucket_name, part_iterator in tests:
            with self.subTest(replica_name):
                count = 0
                for part_number, data in part_iterator(bucket_name, multipart['key'], threads=1):
                    self.assertEqual(expected_parts[part_number], data)
                    count += 1
                self.assertEqual(number_of_parts, count)

    def test_tags(self):
        key = f"{uuid4()}"
        tests = [("aws", s3_blobstore, s3_test_bucket, self._put_s3_obj),
                 ("gcp", gs_blobstore, gs_test_bucket, self._put_gs_obj)]
        for replica_name, blobstore, bucket_name, upload in tests:
            upload(bucket_name, key, b"")
            tags = dict(foo="bar", doom="gloom")
            blobstore.blob(key).put_tags(tags)
            self.assertEqual(tags, blobstore.blob(key).get_tags())

    def test_exists(self):
        key = f"{uuid4()}"
        tests = [("aws", s3_blobstore, s3_test_bucket, self._put_s3_obj),
                 ("gcp", gs_blobstore, gs_test_bucket, self._put_gs_obj)]
        for replica_name, blobstore, bucket_name, upload in tests:
            with self.subTest(replica=replica_name):
                self.assertFalse(blobstore.blob(key).exists())
                upload(bucket_name, key, b"")
                self.assertTrue(blobstore.blob(key).exists())

    def _put_s3_obj(self, bucket, key, data):
        blob = aws.resource("s3").Bucket(bucket).Object(key)
        blob.upload_fileobj(io.BytesIO(data))
        return blob

    def _put_gs_obj(self, bucket, key, data):
        blob = _client().bucket(bucket).blob(key)
        blob.upload_from_file(io.BytesIO(data))
        return blob

    def test_s3_multipart_writer(self):
        expected_data = test_data.multipart
        for threads in [None, 2]:
            with self.subTest(threads=threads):
                chunk_size = get_s3_multipart_chunk_size(len(expected_data))
                number_of_chunks = ceil(len(expected_data) / chunk_size)
                key = f"{uuid4()}"
                start_time = time.time()
                with S3MultipartWriter(s3_test_bucket, key, threads) as writer:
                    for i in range(number_of_chunks):
                        part = Part(number=i,
                                    data=expected_data[i * chunk_size: (i + 1) * chunk_size])
                        writer.put_part(part)
                print(f"upload duration threads={threads}", time.time() - start_time)
                retrieved_data = aws.resource("s3").Bucket(s3_test_bucket).Object(key).get()['Body'].read()
                self.assertEqual(expected_data, retrieved_data)

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

if __name__ == '__main__':
    unittest.main()
