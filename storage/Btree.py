
from storage import ASC, DESC
import btree

from storage.Key import Key

DEFAULT_CACHE_SIZE = 100 * 1024 # 100 kB

KEY_DELIMITER = '\x1C'
START_DELIMITER = '\x00'
END_DELIMITER = '\x1F'

class Btree(Base):
    """ Data implements a datastore """

    def __init__(self, store_path: str, cache_size=DEFAULT_CACHE_SIZE):
        try:
            f = open(store_path, "r+b")
        except OSError:
            f = open(store_path, "w+b")
        self._file = f
        self._path = store_path

        db = btree.open(f, cachesize=cache_size)
        self.db = db

    def __del__(self):
        self.close()

    def close(self):
        self.db.flush()
        self.db.close()
        self._file.close()

    def _serialize_key(self, k: Key) -> str:
        bytes(k.device_id, 'utf-8') + \
        KEY_DELIMITER + \
        bytes(str(k.time), 'utf-8') + \
        KEY_DELIMITER + \
        bytes(k.path, 'utf-8') + \
        KEY_DELIMITER + \
        bytes(k.key, 'utf-8')

    def read(self, key: Key) -> bytes:
        k = self._serialize_key(key)
        return self.db[k]

    def write(self, key: Key, value: bytes):
        k = self._serialize_key(key)
        return self.db[k] = value

    def getRange(self, start: bytes, end: bytes, sort_order: int = DESC) -> dict_items:
        return self.db.items(start, end) if sort_order == ASC else self.db.items(start, end, btree.DESC)
