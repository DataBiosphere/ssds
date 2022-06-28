#!/usr/bin/env python
import io
import os
import sys
from functools import lru_cache
from typing import Tuple, Any

import boto3
from google.cloud.storage import Client

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))  # noqa
sys.path.insert(0, pkg_root)  # noqa

from ssds.blobstore import AWS_MIN_CHUNK_SIZE
from ssds import aws


multfile_layout = [
    ("EMPTY", "zero_byte_file_1.dat", ("",)),
    ("ONESHOT", "bert.dat", ("",)),
    ("ONESHOT", "earny.dat", ("",)),
    ("ONESHOT", "phill.dat", ("subdir1",)),
    ("ONESHOT", "jackson.dat", ("subdir1",)),
    ("ONESHOT", "dwight.dat", ("subdir2",)),
    ("ONESHOT", "howard.dat", ("subdir2",)),
    ("ONESHOT", "rosy.dat", ("subdir1", "subsubdir")),
    ("ONESHOT", "jones.dat", ("subdir1", "subsubdir")),
    ("ONESHOT", "sally.dat", ("singelfile",)),
    ("MULTIPART", "multipart.dat", ("",)),
]

def _prepare_multifile_submission(root: str, oneshot_data: bytes, multipart_data: bytes):
    tree = list()
    for data_style, filename, subdirs in multfile_layout:
        dirname = os.path.join(root, *subdirs)
        filepath = os.path.join(dirname, filename)
        try:
            os.mkdir(dirname)
        except FileExistsError:
            pass
        if "EMPTY" == data_style:
            data = b""
        elif "ONESHOT" == data_style:
            data = oneshot_data
        elif "MULTIPART" == data_style:
            data = multipart_data
        else:
            raise ValueError(f"Can't write fixture data for part layout {data_style}")
        _write_and_upload(filepath, data)
        tree.append((data_style, filepath, _remote_key(filepath)))
    return tree

def _write_and_upload(filepath: str, data: bytes):
    remote_key = _remote_key(filepath)
    with open(filepath, "wb") as fh:
        fh.write(data)
    _s3_bucket().Object(remote_key).upload_fileobj(io.BytesIO(data))
    _gs_bucket().blob(remote_key).upload_from_file(io.BytesIO(data))

def _remote_key(d: str) -> str:
    if d.startswith("/"):
        return d[1:]
    else:
        return d

@lru_cache()
def _s3_bucket():
    return aws.resource("s3").Bucket("org-hpp-ssds-upload-test")

@lru_cache()
def _gs_bucket():
    return Client().bucket("org-hpp-ssds-upload-test")

def populate_fixtures(dirname: str, oneshot_data: bytes, multipart_data: bytes) -> Tuple[Any, str]:
    tree = _prepare_multifile_submission(dirname, oneshot_data, multipart_data)
    return tree, _remote_key(dirname)

if __name__ == "__main__":
    oneshot_data = os.urandom(7)
    multipart_data = os.urandom(2 * AWS_MIN_CHUNK_SIZE + 1)
    # populate_fixtures(tempfile.TemporaryDirectory(oneshot_data, multipart_data))
