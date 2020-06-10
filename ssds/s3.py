import io
import os
import typing
from math import ceil

from ssds import aws, checksum

MiB = 1024 ** 2

AWS_MIN_CHUNK_SIZE = 64 * MiB
"""Files must be larger than this before we consider multipart uploads."""

MULTIPART_THRESHOLD = AWS_MIN_CHUNK_SIZE + 1
"""Convenience variable for Boto3 TransferConfig(multipart_threhold=)."""

AWS_MAX_MULTIPART_COUNT = 10000
"""Maximum number of parts allowed in a multipart upload.  This is a limitation imposed by S3."""


def get_s3_multipart_chunk_size(filesize: int):
    """Returns the chunk size of the S3 multipart object, given a file's size."""
    if filesize <= AWS_MAX_MULTIPART_COUNT * AWS_MIN_CHUNK_SIZE:
        return AWS_MIN_CHUNK_SIZE
    else:
        raw_part_size = ceil(filesize / AWS_MAX_MULTIPART_COUNT)
        part_size_in_integer_megabytes = ((raw_part_size + MiB - 1) // MiB) * MiB
        return part_size_in_integer_megabytes

def upload_object(filepath: str, bucket: str, key: str):
    size = os.stat(filepath).st_size
    chunk_size = get_s3_multipart_chunk_size(size)
    if chunk_size >= size:
        s3_etag, gs_crc32c = _upload_oneshot(filepath, bucket, key)
    else:
        s3_etag, gs_crc32c = _upload_multipart(filepath, bucket, key, chunk_size)
    print(f"s3://{bucket}/{key}")
    tags = dict(TagSet=[dict(Key="SSDS_MD5", Value=s3_etag), dict(Key="SSDS_CRC32C", Value=gs_crc32c)])
    aws.client("s3").put_object_tagging(Bucket=bucket, Key=key, Tagging=tags)

def _upload_oneshot(filepath: str, bucket: str, key: str):
    blob = aws.resource("s3").Bucket(bucket).Object(key)
    with open(filepath, "rb") as fh:
        data = fh.read()
    gs_crc32c = checksum.crc32c(data).google_storage_crc32c()
    s3_etag = checksum.md5(data).hexdigest()
    blob.upload_fileobj(io.BytesIO(data))
    assert s3_etag == blob.e_tag.strip("\"")
    return s3_etag, gs_crc32c

def _upload_multipart(filepath: str, bucket: str, key: str, part_size: int):
    mpu = aws.client("s3").create_multipart_upload(Bucket=bucket, Key=key)['UploadId']
    with open(filepath, "rb") as fh:
        try:
            info = _copy_parts(mpu, bucket, key, fh, part_size)
        except Exception:
            aws.client("s3").abort_multipart_upload(Bucket=bucket, Key=key, UploadId=mpu)
            raise
    aws.client("s3").complete_multipart_upload(Bucket=bucket,
                                               Key=key,
                                               MultipartUpload=dict(Parts=info['parts']),
                                               UploadId=mpu)
    s3_etag = aws.resource("s3").Bucket(bucket).Object(key).e_tag.strip("\"")
    bin_md5 = b"".join([checksum.binascii.unhexlify(part['ETag'].strip("\""))
                        for part in info['parts']])
    composite_etag = checksum.md5(bin_md5).hexdigest() + "-" + str(len(info['parts']))
    assert composite_etag == aws.resource("s3").Bucket(bucket).Object(key).e_tag.strip("\"")
    return s3_etag, info['gs_crc32c']

def _copy_parts(mpu: str, bucket: str, key: str, fileobj: typing.BinaryIO, part_size: int):
    part_number = 0
    parts = []
    crc32c = checksum.crc32c(b"")
    while True:
        data = fileobj.read(part_size)
        if not data:
            break
        part_number += 1
        resp = aws.client("s3").upload_part(
            Body=data,
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=mpu,
        )
        computed_etag = checksum.md5(data).hexdigest()
        crc32c.update(data)
        assert computed_etag == resp['ETag'].strip("\"")
        parts.append(dict(ETag=resp['ETag'], PartNumber=part_number))
    return dict(gs_crc32c=crc32c.google_storage_crc32c(), parts=parts)

def list(bucket: str, prefix=""):
    for item in aws.resource("s3").Bucket(bucket).objects.filter(Prefix=prefix):
        yield item.key
