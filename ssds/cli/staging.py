"""
Upload, sync, and query staging area
"""
import os
import argparse
from concurrent.futures import ThreadPoolExecutor

from ssds import sync
from ssds.deployment import Staging
from ssds.cli import dispatch


staging_cli = dispatch.group("staging", help=__doc__, arguments={
    "--deployment": dict(type=str, default=Staging.default.name, help="SSDS Deployment")
})

@staging_cli.command("upload", arguments={
    "--submission-id": dict(type=str, required=True, help="Submission id provided for your submission"),
    "--name": dict(type=str, default=None, help="Human readable name of submission. Cannot contain spaces"),
    "path": dict(type=str, help="Directory containing submission material"),
})
def upload(args: argparse.Namespace):
    """
    Upload a local directory tree to the staging bucket.
    Existing files in the submission will be overwritten.
    """
    ssds = Staging[args.deployment].ssds
    root = os.path.abspath(os.path.normpath(args.path))
    with ThreadPoolExecutor(max_workers=4) as e:
        for ssds_key in ssds.upload(root, args.submission_id, args.name, e):
            print(ssds.compose_blobstore_url(ssds_key))

@staging_cli.command("list")
def list(args: argparse.Namespace):
    """
    List submissions in the staging bucket"
    """
    ssds = Staging[args.deployment].ssds
    for submission_id, submission_name in ssds.list():
        print(submission_id, submission_name)

@staging_cli.command("list-submission", arguments={
    "--submission-id": dict(type=str, required=True, help="id of submission")
})
def list_submission(args: argparse.Namespace):
    ssds = Staging[args.deployment].ssds
    submission_exists = False
    for ssds_key in ssds.list_submission(args.submission_id):
        submission_exists = True
        print(ssds.compose_blobstore_url(ssds_key))
    if not submission_exists:
        print(f"No submission found for {args.submission_id}")

@staging_cli.command("sync", arguments={
    "--dst-deployment": dict(type=str, default="gcp", help="destination deployment"),
    "--submission-id": dict(type=str, required=True, help="id of submission")
})
def sync_command(args: argparse.Namespace):
    """
    Copy all files for `submission-id` from `deployment` to `dst-deployment`.
    """
    src = Staging[args.deployment].ssds
    dst = Staging[args.dst_deployment].ssds
    src_bucket = f"{src.blobstore.schema}{src.bucket}"
    dst_bucket = f"{dst.blobstore.schema}{dst.bucket}"
    for key in sync(args.submission_id, src, dst):
        print(f"Syncing: {src_bucket}/{key} -> {dst_bucket}/{key}")

@staging_cli.command("bucket")
def get_bucket(args: argparse.Namespace):
    """
    Print bucket of SSDS deployment
    """
    ssds = Staging[args.deployment].ssds
    print(f"{ssds.blobstore.schema}{ssds.bucket}")

@staging_cli.command("release")
def release(args: argparse.Namespace):
    raise NotImplementedError()
