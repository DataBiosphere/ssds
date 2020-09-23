#!/usr/bin/env python
import io
import os
import sys
import time
import tempfile
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
from ssds.blobstore.local import LocalBlobStore
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

class TestBlobStore(infra.SuppressWarningsMixin, unittest.TestCase):
    def test_schema(self):
        self.assertEqual("s3://", S3BlobStore.schema)
        self.assertEqual("gs://", GSBlobStore.schema)

    def test_put_get(self):
        key = f"{uuid4()}"
        expected_data = test_data.oneshot
        tests = [("aws", self._put_s3_obj, s3_test_bucket, s3_blobstore),
                 ("gcp", self._put_gs_obj, gs_test_bucket, gs_blobstore),
                 ("local", self._put_local_obj, local_test_bucket, local_blobstore)]
        for test_name, upload, bucket_name, bs in tests:
            with self.subTest(test_name):
                bs.blob(key).put(expected_data)
                data = bs.blob(key).get()
                self.assertEqual(data, expected_data)
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").get()

    def test_copy_from(self):
        dst_key = f"{uuid4()}"
        oneshot, multipart = test_data.uploaded([local_blobstore, s3_blobstore, gs_blobstore])
        for bs in (local_blobstore, s3_blobstore, gs_blobstore):
            for src_key in (oneshot['key'], multipart['key']):
                with self.subTest(src_key=src_key, dst_key=dst_key, blobstore=bs):
                    src_blob = bs.blob(src_key)
                    dst_blob = bs.blob(dst_key)
                    dst_blob.copy_from(src_blob)
                    self.assertEqual(src_blob.get(), dst_blob.get())
            with self.subTest("blob not found", blobstore=bs):
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").copy_from(bs.blob(f"{uuid4()}"))

    def test_download(self):
        oneshot, _ = test_data.uploaded([local_blobstore, s3_blobstore, gs_blobstore])
        src_key = oneshot['key']
        dst_path = local_blobstore.blob(f"{uuid4()}").url
        for bs in (local_blobstore, s3_blobstore, gs_blobstore):
            with self.subTest(src_key=src_key, blobstore=bs):
                bs.blob(src_key).download(dst_path)
                with open(dst_path, "rb") as fh:
                    data = fh.read()
                self.assertEqual(oneshot['data'], data)
            with self.subTest("blob not found", blobstore=bs):
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").download(dst_path)
                    
    def test_size(self):
        expected_size = randint(1, 10)
        data = os.urandom(expected_size)
        tests = [("aws", self._put_s3_obj, s3_test_bucket, s3_blobstore),
                 ("gcp", self._put_gs_obj, gs_test_bucket, gs_blobstore),
                 ("local", self._put_local_obj, local_test_bucket, local_blobstore)]
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
        for test_name, bs in tests:
            with self.subTest(test_name):
                count = 0
                for part_number, data in bs.blob(key).parts():
                    self.assertEqual(expected_parts[part_number], data)
                    count += 1
                self.assertEqual(number_of_parts, count)
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").parts()

    def test_tags(self):
        tags = dict(foo="bar", doom="gloom")
        tests = [("aws", self._put_s3_obj, s3_test_bucket, s3_blobstore),
                 ("gcp", self._put_gs_obj, gs_test_bucket, gs_blobstore)]
        for test_name, upload, bucket_name, bs in tests:
            with self.subTest(test_name):
                key = upload(bucket_name, b"")
                bs.blob(key).put_tags(tags)
                self.assertEqual(tags, bs.blob(key).get_tags())
                with self.assertRaises(BlobNotFoundError):
                    bs.blob(f"{uuid4()}").put_tags(tags)

    def test_exists(self):
        key = f"{uuid4()}"
        tests = [("aws", self._put_s3_obj, s3_test_bucket, s3_blobstore),
                 ("gcp", self._put_gs_obj, gs_test_bucket, gs_blobstore)]
        for test_name, upload, bucket_name, bs in tests:
            with self.subTest(replica=test_name):
                self.assertFalse(bs.blob(key).exists())
                upload(bucket_name, b"", key=key)
                self.assertTrue(bs.blob(key).exists())

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

    def _put_local_obj(self, bucket: str, data: bytes, key: Optional[str]=None) -> str:
        if not hasattr(self, "tempdir"):
            self.tempdir = tempfile.TemporaryDirectory()
        path = os.path.join(self.tempdir.name, f"{uuid4()}")
        with open(path, "wb") as fh:
            fh.write(data)
        return path

    def test_multipart_writers(self):
        expected_data = test_data.multipart
        chunk_size = get_s3_multipart_chunk_size(len(expected_data))
        number_of_chunks = ceil(len(expected_data) / chunk_size)
        for bs in (s3_blobstore, gs_blobstore):
            with self.subTest(blobstore=bs):
                dst_blob = bs.blob(f"{uuid4()}")
                with dst_blob.multipart_writer() as writer:
                    for i in range(number_of_chunks):
                        part = Part(number=i,
                                    data=expected_data[i * chunk_size: (i + 1) * chunk_size])
                        writer.put_part(part)
                self.assertEqual(expected_data, dst_blob.get())

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
