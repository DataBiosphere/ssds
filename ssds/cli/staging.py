"""
Upload, sync, and query staging area
"""
import os
import argparse

import ssds
from ssds import s3, gs
from ssds.config import Config, Platform
from ssds.cli import dispatch

staging_cli = dispatch.group("staging", help=__doc__)

@staging_cli.command("upload", arguments={
    "--submission-id": dict(type=str, required=True, help="Submission id provided for your submission"),
    "--name": dict(type=str, required=True, help="Human readable name of submission. Cannot contain spaces"),
    "path": dict(type=str, help="Directory containing submission material"),
})
def upload(args: argparse.Namespace):
    """
    Upload a local directory tree to the staging bucket
    """
    root = os.path.abspath(os.path.normpath(args.path))
    ssds.upload(root, args.submission_id, args.name)

@staging_cli.command("list")
def list(args: argparse.Namespace):
    """
    List submissions in the staging bucket"
    """
    if Platform.AWS == Config.platform:
        listing = s3.list(Config.staging_bucket)
    else:
        listing = gs.list(Config.staging_bucket)
    prev_submission_id = ""
    for key in listing:
        try:
            submission_id, submission_name, _ = key.split("--", 2)
        except ValueError:
            continue
        if submission_id != prev_submission_id:
            print(submission_name, submission_id)
            prev_submission_id = submission_id

@staging_cli.command("release")
def release(args: argparse.Namespace):
    raise NotImplementedError()
