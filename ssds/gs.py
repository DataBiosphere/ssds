import io
import os
import warnings
from functools import lru_cache

import gs_chunked_io as gscio
from google.cloud.storage import Client

from ssds import checksum
from ssds.s3 import get_s3_multipart_chunk_size


# Suppress the annoying google gcloud _CLOUD_SDK_CREDENTIALS_WARNING warnings
warnings.filterwarnings("ignore", "Your application has authenticated using end user credentials")


schema = "gs://"

@lru_cache()
def client():
    if not os.environ.get('GOOGLE_CLOUD_PROJECT'):
        raise RuntimeError("Please set the GOOGLE_CLOUD_PROJECT environment variable")
    return Client()

def upload_object(filepath: str, bucket: str, key: str):
    size = os.stat(filepath).st_size
    chunk_size = get_s3_multipart_chunk_size(size)
    if chunk_size >= size:
        s3_etag, gs_crc32c = _upload_oneshot(filepath, bucket, key)
    else:
        s3_etag, gs_crc32c = _upload_multipart(filepath, bucket, key, chunk_size)
    blob = client().bucket(bucket).blob(key)
    blob.metadata = dict(SSDS_MD5=s3_etag, SSDS_CRC32C=gs_crc32c)
    blob.patch()

def _upload_oneshot(filepath: str, bucket: str, key: str):
    blob = client().bucket(bucket).blob(key)
    with open(filepath, "rb") as fh:
        data = fh.read()
        gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
        s3_etag = checksum.md5(data).hexdigest()
        blob.upload_from_file(io.BytesIO(data))
    blob.reload()
    assert gs_crc32c == blob.crc32c
    return s3_etag, gs_crc32c

def _upload_multipart(filepath: str, bucket_name: str, key: str, part_size: int):
    _crc32c = checksum.crc32c(b"")
    _s3_etags = []

    def _put_part(part_number: int, part_name: str, data: bytes):
        _crc32c.update(bytes(data))
        _s3_etags.append(checksum.md5(data).hexdigest())

    bucket = client().bucket(bucket_name)
    with gscio.writer.Writer(key, bucket, part_size, part_callback=_put_part) as writer:
        with open(filepath, "rb") as fh:
            while True:
                data = fh.read(part_size)
                if data:
                    writer.write(data)
                else:
                    break

    s3_etag = checksum.compute_composite_etag(_s3_etags)
    gs_crc32c = _crc32c.google_storage_crc32c()
    assert gs_crc32c == bucket.get_blob(key).crc32c
    return s3_etag, gs_crc32c

def list(bucket_name: str, prefix=""):
    for blob in client().bucket(bucket_name).list_blobs(prefix=prefix):
        yield blob.name
