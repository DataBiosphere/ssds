#!/usr/bin/env python
import io
import os
import sys
import time
import unittest
from math import ceil
from uuid import uuid4
from random import randint
from typing import Optional

from google.cloud import storage

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from ssds import aws, checksum
from ssds.blobstore import (AWS_MIN_CHUNK_SIZE, AWS_MAX_MULTIPART_COUNT, MiB, get_s3_multipart_chunk_size, Part,
                            BlobNotFoundError)
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
        expected_data = test_data.oneshot
        tests = [("aws", self._put_s3_obj, s3_test_bucket, S3BlobStore),
                 ("gcp", self._put_gs_obj, gs_test_bucket, GSBlobStore)]
        for test_name, upload, bucket_name, bs in tests:
            with self.subTest(test_name):
                key = upload(bucket_name, expected_data)
                data = bs(bucket_name).blob(key).get()
                self.assertEqual(data, expected_data)
                with self.assertRaises(BlobNotFoundError):
                    bs(bucket_name).blob(f"{uuid4()}").get()

    def _test_put(self):
        """
        This is implicitly tested during `TestSSDS`.
        """

    def test_size(self):
        expected_size = randint(1, 10)
        data = os.urandom(expected_size)
        tests = [("aws", self._put_s3_obj, s3_test_bucket, s3_blobstore),
                 ("gcp", self._put_gs_obj, gs_test_bucket, gs_blobstore)]
        for test_name, upload, bucket_name, bs in tests:
            with self.subTest(test_name):
                key = upload(bucket_name, data)
                self.assertEqual(expected_size, bs.blob(key).size())
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").size()

    def test_cloud_native_checksums(self):
        data = os.urandom(1)
        s3_etag = checksum.md5(data).hexdigest()
        crc32c = checksum.crc32c(data).google_storage_crc32c()
        tests = [("aws", self._put_s3_obj, s3_test_bucket, s3_blobstore, s3_etag),
                 ("gcp", self._put_gs_obj, gs_test_bucket, gs_blobstore, crc32c)]
        for test_name, upload, bucket_name, bs, expected_checksum in tests:
            with self.subTest(test_name):
                key = upload(bucket_name, data)
                self.assertEqual(expected_checksum, bs.blob(key).cloud_native_checksum())
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").cloud_native_checksum()

    def test_part_iterators(self):
        _, multipart = test_data.uploaded([s3_blobstore, gs_blobstore])
        key = multipart['key']
        chunk_size = get_s3_multipart_chunk_size(len(multipart['data']))
        number_of_parts = ceil(len(multipart['data']) / chunk_size)
        expected_parts = [multipart['data'][i * chunk_size:(i + 1) * chunk_size]
                          for i in range(number_of_parts)]
        tests = [("aws", s3_blobstore), ("gcp", gs_blobstore)]
        for replica_name, bs in tests:
            with self.subTest(replica_name):
                count = 0
                for part_number, data in bs.blob(key).parts(threads=1):
                    self.assertEqual(expected_parts[part_number], data)
                    count += 1
                self.assertEqual(number_of_parts, count)
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").parts(threads=1)

    def test_tags(self):
        tags = dict(foo="bar", doom="gloom")
        tests = [("aws", s3_blobstore, s3_test_bucket, self._put_s3_obj),
                 ("gcp", gs_blobstore, gs_test_bucket, self._put_gs_obj)]
        for replica_name, bs, bucket_name, upload in tests:
            with self.subTest(replica_name):
                key = upload(bucket_name, b"")
                bs.blob(key).put_tags(tags)
                self.assertEqual(tags, bs.blob(key).get_tags())
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").put_tags(tags)

    def test_exists(self):
        key = f"{uuid4()}"
        tests = [("aws", s3_blobstore, s3_test_bucket, self._put_s3_obj),
                 ("gcp", gs_blobstore, gs_test_bucket, self._put_gs_obj)]
        for replica_name, blobstore, bucket_name, upload in tests:
            with self.subTest(replica=replica_name):
                self.assertFalse(blobstore.blob(key).exists())
                upload(bucket_name, b"", key=key)
                self.assertTrue(blobstore.blob(key).exists())

    def _put_s3_obj(self, bucket: str, data: bytes, key: Optional[str]=None) -> str:
        key = key or f"{uuid4()}"
        blob = aws.resource("s3").Bucket(bucket).Object(key)
        blob.upload_fileobj(io.BytesIO(data))
        return key

    def _put_gs_obj(self, bucket: str, data: bytes, key: Optional[str]=None) -> str:
        key = key or f"{uuid4()}"
        blob = _client().bucket(bucket).blob(key)
        blob.upload_from_file(io.BytesIO(data))
        return key

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
