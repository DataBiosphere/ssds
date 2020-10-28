import os
import sys
import json
import logging
from typing import Any, Dict, Generator, List, Optional, Tuple, Type, Union

from ssds import storage, aws, gcp, utils
from ssds.blobstore import BlobStore
from ssds.blobstore.s3 import S3Blob, S3BlobStore
from ssds.blobstore.gs import GSBlob, GSBlobStore
from ssds.blobstore.local import LocalBlob, LocalBlobStore


logger = logging.getLogger(__name__)

MAX_KEY_LENGTH = 1024  # this is the maximum length for S3 and GS object names
# GS docs: https://cloud.google.com/storage/docs/naming-objects
# S3 docs: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html

class SSDS:
    blobstore_class: Type[BlobStore]
    bucket: str
    prefix = "submissions"
    release_prefix = "working"
    _name_delimeter = "--"  # Not using "/" as name delimeter produces friendlier `aws s3` listing

    def __init__(self, google_billing_project: Optional[str]=None):
        kwargs = dict()
        if google_billing_project is not None:
            if self.blobstore_class != GSBlobStore:
                raise ValueError("google_billing_project may only be passed in for Google Storage deployments")
            kwargs['billing_project'] = google_billing_project
        # TODO: figure out how to use type checking on this line
        self.blobstore = self.blobstore_class(self.bucket, **kwargs)  # type: ignore

    def list(self) -> Generator[Tuple[str, str], None, None]:
        listing = self.blobstore.list(self.prefix)
        prev_submission_id = ""
        for key in listing:
            try:
                ssds_key = key.strip(f"{self.prefix}/")
                submission_id, parts = ssds_key.split(self._name_delimeter, 1)
                submission_name, _ = parts.split("/", 1)
            except ValueError:
                continue
            if submission_id != prev_submission_id:
                yield submission_id, submission_name
                prev_submission_id = submission_id

    def __repr__(self) -> str:
        return f"<SSDS {self.__class__.__name__} {self.blobstore_class.schema}{self.bucket}>"

    def __str__(self) -> str:
        return self.__repr__()

    def list_submission(self, submission_id: str) -> Generator[str, None, None]:
        for blob in self.blobstore.list(f"{self.prefix}/{submission_id}"):
            ssds_key = blob.key.replace(f"{self.prefix}/", "", 1)
            yield ssds_key

    def get_submission_name(self, submission_id: str) -> Optional[str]:
        name = None
        for blob in self.blobstore.list(f"{self.prefix}/{submission_id}"):
            ssds_key = blob.key.strip(f"{self.prefix}/")
            _, parts = ssds_key.split(self._name_delimeter, 1)
            name, _ = parts.split("/", 1)
            break
        return name

    def upload(self,
               src_url: str,
               submission_id: str,
               name: Optional[str]=None,
               subdir: Optional[str]=None) -> Generator[str, None, None]:
        """
        Upload files from src_url directory and yield ssds_key for each file.
        This returns a generator that must be iterated for uploads to occur.
        """
        name = self._check_name_exists(submission_id, name)
        assert " " not in name  # TODO: create regex to enforce name format?
        assert self._name_delimeter not in name  # TODO: create regex to enforce name format?
        pfx, listing = storage.listing_for_url(src_url)
        pfx = pfx.strip("/")
        subdir = f"{subdir.strip('/')}" if subdir else ""
        with storage.CopyClient() as cc:
            for src_blob in listing:
                path = src_blob.key.replace(pfx, subdir, 1)
                ssds_key = self._compose_ssds_key(submission_id, name, path)
                dst_blob = self.blobstore.blob(f"{self.prefix}/{ssds_key}")
                cc.copy_compute_checksums(src_blob, dst_blob)
                for src_blob, dst_blob, exception in cc.completed():
                    if exception is None:
                        yield dst_blob.key[len(f"{self.prefix}/"):]
        for src_blob, dst_blob, exception in cc.completed():
            if exception is None:
                yield dst_blob.key[len(f"{self.prefix}/"):]
        logger.info(f"Completed upload: src_url='{src_url}' "
                    f"submission_id='{submission_id}' "
                    f"name='{name}' "
                    f"subdir='{subdir}'")

    def copy(self, src_url: str, submission_id: str, name: str, submission_path: str):
        """
        Copy files from local or cloud locations into the ssds, computing checksums.
        """
        name = self._check_name_exists(submission_id, name)
        ssds_key = self._compose_ssds_key(submission_id, name, submission_path)
        src_blob = storage.blob_for_url(src_url)
        dst_blob = self.blobstore.blob(f"{self.prefix}/{ssds_key}")
        storage.copy_compute_checksums(src_blob, dst_blob)

    def _check_name_exists(self, submission_id: str, name: Optional[str]) -> str:
        existing_name = self.get_submission_name(submission_id)
        if not name:
            if not existing_name:
                raise ValueError("Must provide name for new submissions")
            name = existing_name
        elif existing_name and existing_name != name:
            raise ValueError("Cannot update name of existing submission")
        return name

    def _compose_ssds_key(self, submission_id: str, submission_name: str, path: str) -> str:
        ssds_key = f"{submission_id}{self._name_delimeter}{submission_name}/{path.strip('/')}"
        blobstore_key = f"{self.prefix}/{ssds_key}"
        if MAX_KEY_LENGTH <= len(blobstore_key):
            raise ValueError(f"Total key length must not exceed {MAX_KEY_LENGTH} characters {os.linesep}"
                             f"{blobstore_key} is too long {os.linesep}"
                             f"Use a shorter submission name")
        return ssds_key

    def compose_blobstore_url(self, ssds_key: str) -> str:
        return f"{self.blobstore.schema}{self.bucket}/{self.prefix}/{ssds_key}"

def is_synced(src_blob: storage.AnyBlob, dst_blob: storage.AnyBlob) -> bool:
    if dst_blob.exists():
        return dst_blob.get_tags() == src_blob.get_tags()
    else:
        return False

def sync(submission_id: str, src: SSDS, dst: SSDS) -> Generator[str, None, None]:
    with storage.CopyClient() as cc:
        for src_blob in src.blobstore.list(f"{src.prefix}/{submission_id}"):
            dst_blob = dst.blobstore.blob(src_blob.key)
            if is_synced(src_blob, dst_blob):
                logger.info(f"already-synced {src_blob.key} from {src} to {dst}")
            else:
                logger.info(f"syncing {src_blob.key} from {src} to {dst}")
                cc.copy(src_blob, dst_blob)
            for src_blob, dst_blob, exception in cc.completed():
                if exception is None:
                    yield dst_blob.key
    for src_blob, dst_blob, exception in cc.completed():
        if exception is None:
            yield dst_blob.key
    logger.info(f"Completed sync: "
                f"submission_id='{submission_id}' "
                f"src='{src}' "
                f"dst='{dst}'")

def release(submission_id: str, src: SSDS, dst: SSDS, transfers: List[Tuple[str, str]]) -> dict:
    # verify transfers
    submission_name = src.get_submission_name(submission_id)
    assert submission_name is not None, f"No submission for {submission_id}"
    dst_keys = set()
    src_keys = set()
    for src_url, dst_url in transfers:
        src_bucket, src_key = storage.parse_cloud_url(src_url)
        dst_bucket, dst_key = storage.parse_cloud_url(dst_url)
        assert src_bucket == src.bucket, f"Source keys must be from bucket {src.bucket}"
        assert dst_bucket == dst.bucket, f"Destination keys must be from bucket {dst.bucket}"
        assert src_key not in src_keys, "Duplicate source keys not allowed"
        assert dst_key not in dst_keys, "Duplicate destination keys not allowed"
        assert src_key.startswith(f"submissions/{submission_id}"), f"Source keys must be in submission {submission_id}"
        assert dst_key.startswith(f"{src.release_prefix}/"), \
               f"Destination keys must be placed into the prefix '{src.release_prefix}/'"
        src_keys.add(src_key)
        dst_keys.add(dst_key)

    manifest: Dict[str, Any] = dict(submission_id=submission_id,
                                    src_bucket=src.bucket,
                                    dst_bucket=dst.bucket,
                                    aws_identity=aws.get_identity(),
                                    gcp_identity=gcp.get_identity(),
                                    transfer_map=list())

    # perform transfer and write manifest
    manifest['start_timestamp'] = utils.timestamp_now()
    with storage.CopyClient() as cc:
        for src_url, dst_url in transfers:
            src_blob = storage.blob_for_url(src_url)
            dst_blob = storage.blob_for_url(dst_url)
            if not is_synced(src_blob, dst_blob):
                cc.copy(src_blob, dst_blob)
    for src_blob, dst_blob, exception in cc.completed():
        if exception is None:
            manifest['transfer_map'].append(dict(src_key=src_blob.key, dst_key=dst_blob.key))
    manifest['end_timestamp'] = utils.timestamp_now()
    ssds_key = src._compose_ssds_key(submission_id,
                                     submission_name,
                                     f"release-transfer-manifests/{manifest['start_timestamp']}")
    key = f"{src.prefix}/{ssds_key}"
    if 0 < len(manifest['transfer_map']):
        src.blobstore.blob(key).put(json.dumps(manifest).encode("utf-8"))
    return manifest
