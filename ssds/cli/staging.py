"""
Upload, sync, and query staging area
"""
import os
import argparse

import ssds
from ssds.deployment import Staging
from ssds.cli import dispatch


staging_cli = dispatch.group("staging", help=__doc__)

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
    root = os.path.abspath(os.path.normpath(args.path))
    for ssds_key in Staging.default.ssds.upload(root, args.submission_id, args.name):
        print(Staging.default.ssds.compose_blobstore_url(ssds_key))

@staging_cli.command("list")
def list(args: argparse.Namespace):
    """
    List submissions in the staging bucket"
    """
    for submission_id, submission_name in Staging.default.ssds.list():
        print(submission_id, submission_name)

@staging_cli.command("list-submission", arguments={
    "--submission-id": dict(type=str, required=True, help="id of submission")
})
def list_submission(args: argparse.Namespace):
    submission_exists = False
    for ssds_key in Staging.default.ssds.list_submission(args.submission_id):
        submission_exists = True
        print(Staging.default.ssds.compose_blobstore_url(ssds_key))
    if not submission_exists:
        print(f"No submission found for {args.submission_id}")

@staging_cli.command("release")
def release(args: argparse.Namespace):
    raise NotImplementedError()
