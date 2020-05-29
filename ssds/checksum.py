import base64
import typing
import binascii
from hashlib import md5

import crc32c as google_crc32c

class crc32c:
    def __init__(self, data: bytes=None):
        self._checksum = google_crc32c.Checksum(data)

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
