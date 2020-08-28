import base64
import typing
import binascii
from hashlib import md5
from typing import Tuple, List, Set, Optional

import google_crc32c

class crc32c:
    def __init__(self, data: Optional[bytes]=None):
        if data is not None:
            self._checksum = google_crc32c.Checksum(data)
        else:
            self._checksum = google_crc32c.Checksum(b"")

    def update(self, data: bytes):
        self._checksum.update(data)

    def hexdigest(self) -> str:
        return self._checksum.digest().hex()

    def google_storage_crc32c(self) -> str:
        # Compute the crc32c value assigned to Google Storage objects
        # kind of wonky, right?
        return base64.b64encode(self._checksum.digest()).decode("utf-8")

def compute_composite_etag(etags: typing.List[str]) -> str:
    bin_md5 = b"".join([binascii.unhexlify(etag) for etag in etags])
    composite_etag = md5(bin_md5).hexdigest() + "-" + str(len(etags))
    return composite_etag

class UnorderedChecksum:
    def __init__(self):
        raise NotImplementedError()

    def update(self, chunk_number: int, data: bytes):
        raise NotImplementedError()

    def hexdigest(self) -> str:
        raise NotImplementedError()

class S3EtagUnordered(UnorderedChecksum):
    def __init__(self):
        self._checksums: List[Tuple[int, str]] = list()

    def update(self, chunk_number: int, data: bytes):
        self._checksums.append((chunk_number, md5(data).hexdigest()))

    def hexdigest(self) -> str:
        return compute_composite_etag([cs[1] for cs in sorted(self._checksums)])

class GScrc32cUnordered(UnorderedChecksum):
    def __init__(self):
        self._current_chunk_number = 0
        self._chunks: Set[Tuple[int, bytes]] = set()
        self._checksum = crc32c()

    def update(self, chunk_number: int, data: bytes):
        self._chunks.add((chunk_number, data))
        for chunk_number, data in sorted(self._chunks):
            if chunk_number == self._current_chunk_number:
                self._checksum.update(data)
                self._chunks.remove((chunk_number, data))
                self._current_chunk_number += 1
            else:
                break

    def hexdigest(self) -> str:
        for chunk_number, data in sorted(self._chunks):
            self._checksum.update(data)
        return self._checksum.google_storage_crc32c()
