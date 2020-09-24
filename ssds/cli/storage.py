"""
Cloud agnostic storage CLI
"""
import os
import sys
import logging
import argparse

import ssds
from ssds import storage
from ssds.cli import dispatch


# output logging to stdout
# https://stackoverflow.com/a/56144390
logging.basicConfig()
ssds.logger.level = logging.INFO

storage_cli = dispatch.group("storage", help=__doc__)

@storage_cli.command("cp", arguments={
    "src_url": dict(type=str, help="local path, gs://, or s3://"),
    "dst_url": dict(type=str, help="local path, gs://, or s3://"),
    "--ignore-missing-checksums": dict(default=False,
                                       action="store_true",
                                       help="raise errors on missing checksums"),
})
def cp(args: argparse.Namespace):
    """
    Copy files from the local filesystem or cloud locations into the SSDS
    """
    src_blob = ssds.blob_for_url(args.src_url)
    dst_blob = ssds.blob_for_url(args.dst_url)
    with storage.CopyClient(ignore_missing_checksums=args.ignore_missing_checksums) as client:
        client.copy(src_blob, dst_blob)
