import os
from uuid import uuid4
from typing import Dict, Iterable, Optional

import ssds.blobstore


class TestData:
    def __init__(self, oneshot_size: int=7, multipart_size: int=2 * ssds.blobstore.AWS_MIN_CHUNK_SIZE + 1):
        self.oneshot_size = oneshot_size
        self.multipart_size = multipart_size

        self._oneshot: Optional[bytes] = None
        self._multipart: Optional[bytes] = None
        self._oneshot_key = f"{uuid4()}"
        self._multipart_key = f"{uuid4()}"
        self._uploaded: Dict[ssds.blobstore.BlobStore, bool] = dict()

    @property
    def oneshot(self):
        self._oneshot = self._oneshot or os.urandom(self.oneshot_size)
        return self._oneshot

    @property
    def multipart(self):
        self._multipart = self._multipart or os.urandom(self.multipart_size)
        return self._multipart

    def uploaded(self, dests: Iterable[ssds.blobstore.BlobStore]):
        oneshot = dict(key=self._oneshot_key, data=self.oneshot)
        multipart = dict(key=self._multipart_key, data=self.multipart)
        for bs in dests:
            if not self._uploaded.get(bs):
                for data_config in [oneshot, multipart]:
                    bs.blob(data_config['key']).put(data_config['data'])
                self._uploaded[bs] = True
        return oneshot, multipart
