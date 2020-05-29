import os

from ssds import s3, gs

def upload(src: str, dst_bucket: str, submission_id: str, description: str):
    _upload_local_tree(src, dst_bucket, submission_id, description)

def _upload_local_tree(root: str, dst_bucket: str, submission_id: str, description: str):
    root = os.path.normpath(root)
    assert root == os.path.abspath(root)
    assert "-" not in description  # TODO: create regex to enforce description format?
    assert " " not in description  # TODO: create regex to enforce description format?
    filepaths = [p for p in _list_tree(root)]
    dst_prefix = f"{submission_id}-{description}"
    dst_keys = [f"{dst_prefix}/{os.path.relpath(p, root)}" for p in filepaths]
    if dst_bucket.startswith("s3://"):
        for filepath, dst_key in zip(filepaths, dst_keys):
            s3.upload_object(filepath, dst_bucket[5:], dst_key)
    elif dst_bucket.startswith("gs://"):
        for filepath, dst_key in zip(filepaths, dst_keys):
            gs.upload_object(filepath, dst_bucket[5:], dst_key)
    else:
        raise ValueError(f"Bucket location not supported: {dst_bucket}")

def _list_tree(root):
    for (dirpath, dirnames, filenames) in os.walk(root):
        for filename in filenames:
            relpath = os.path.join(dirpath, filename)
            yield os.path.abspath(relpath)
