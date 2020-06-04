import os

from ssds import s3, gs, config

def upload(src: str, submission_id: str, description: str):
    _upload_local_tree(src, submission_id, description)

def _upload_local_tree(root: str, submission_id: str, description: str):
    root = os.path.normpath(root)
    assert root == os.path.abspath(root)
    assert " " not in description  # TODO: create regex to enforce description format?
    filepaths = [p for p in _list_tree(root)]
    dst_prefix = f"{submission_id}/{description}"
    dst_keys = [f"{dst_prefix}/{os.path.relpath(p, root)}" for p in filepaths]
    if config.Platform.AWS == config.platform:
        for filepath, dst_key in zip(filepaths, dst_keys):
            s3.upload_object(filepath, config.staging_bucket, dst_key)
    elif config.Platform.GCP == config.platform:
        for filepath, dst_key in zip(filepaths, dst_keys):
            gs.upload_object(filepath, config.staging_bucket, dst_key)
    else:
        raise ValueError(f"Unsupported platform: {config.platform}")

def _list_tree(root):
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)
