"""
Upload, sync, and query staging area
"""
import os
import argparse

import ssds
from ssds.cli import dispatch

staging_cli = dispatch.group("staging", help=__doc__)

@staging_cli.command("upload", arguments={
    "--submission-id": dict(type=str, required=True, help="Submission id provided for your submission"),
    "--name": dict(type=str, required=True, help="Human readable name of submission. Cannot contain spaces"),
    "path": dict(type=str, help="Directory containing submission material"),
})
def upload(args: argparse.Namespace):
    root = os.path.abspath(os.path.normpath(args.path))
    ssds.upload(root, args.submission_id, args.name)

@staging_cli.command("release")
def release(args: argparse.Namespace):
    raise NotImplementedError()
