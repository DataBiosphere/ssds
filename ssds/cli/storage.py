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
    ("-r", "--recursive"): dict(default=False, action="store_true", help="copy directories recursively"),
    "--ignore-missing-checksums": dict(default=False,
                                       action="store_true",
                                       help="raise errors on missing checksums"),
})
def cp(args: argparse.Namespace):
    """
    Copy files from the local filesystem or cloud locations into the SSDS
    """
    with storage.CopyClient(ignore_missing_checksums=args.ignore_missing_checksums) as client:
        if not args.recursive:
            src_blob = ssds.blob_for_url(args.src_url)
            dst_blob = ssds.blob_for_url(args.dst_url)
            client.copy(src_blob, dst_blob)
        else:
            src_pfx, listing = ssds.listing_for_url(args.src_url)
            dst_pfx, dst_blobstore = ssds.blobstore_for_url(args.dst_url)
            for src_blob in listing:
                dst_key = storage.transform_key(src_blob.key, src_pfx, dst_pfx)
                dst_blob = dst_blobstore.blob(dst_key)
                client.copy(src_blob, dst_blob)
