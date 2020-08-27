import os
from uuid import uuid4
from typing import Dict, Iterable, Optional

from ssds.blobstore import BlobStore


class TestData:
    _oneshot: Optional[bytes] = None
    _multipart: Optional[bytes] = None
    _uploaded: Dict[BlobStore, bool] = dict()
    _oneshot_key = f"{uuid4()}"
    _multipart_key = f"{uuid4()}"

    @classmethod
    def oneshot(cls):
        cls._oneshot = cls._oneshot or os.urandom(7)
        return cls._oneshot

    @classmethod
    def multipart(cls):
        cls._multipart = cls._multipart or os.urandom(1024 * 1024 * 160)
        return cls._multipart

    @classmethod
    def uploaded(cls, dests: Iterable[BlobStore]):
        oneshot = dict(key=cls._oneshot_key, data=cls.oneshot())
        multipart = dict(key=cls._multipart_key, data=cls.multipart())
        for bs in dests:
            if not cls._uploaded.get(bs):
                for data_config in [oneshot, multipart]:
                    bs.blob(data_config['key']).put(data_config['data'])
                cls._uploaded[bs] = True
        return oneshot, multipart
