import os

from ssds import s3, gs
from ssds.config import Config, Platform

def upload(src: str, submission_id: str, description: str):
    _upload_local_tree(src, submission_id, description)

def _upload_local_tree(root: str, submission_id: str, description: str):
    root = os.path.normpath(root)
    assert root == os.path.abspath(root)
    assert " " not in description  # TODO: create regex to enforce description format?
    assert "--" not in description  # TODO: create regex to enforce description format?
    filepaths = [p for p in _list_tree(root)]
    dst_prefix = f"{submission_id}--{description}"
    dst_keys = [f"{dst_prefix}/{os.path.relpath(p, root)}" for p in filepaths]
    if Platform.AWS == Config.platform:
        for filepath, dst_key in zip(filepaths, dst_keys):
            s3.upload_object(filepath, Config.staging_bucket, dst_key)
    elif Platform.GCP == Config.platform:
        for filepath, dst_key in zip(filepaths, dst_keys):
            gs.upload_object(filepath, Config.staging_bucket, dst_key)
    else:
        raise ValueError(f"Unsupported platform: {Config.platform}")

def _list_tree(root):
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)

def list():
    if Platform.AWS == Config.platform:
        listing = s3.list(Config.staging_bucket)
    else:
        listing = gs.list(Config.staging_bucket)
    prev_submission_id = ""
    for key in listing:
        try:
            submission_id, submission_name = key.split("--", 1)
        except ValueError:
            continue
        if submission_id != prev_submission_id:
            yield submission_id, submission_name
            prev_submission_id = submission_id
