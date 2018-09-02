
from storage import ASC, DESC
import btree

DEFAULT_CACHE_SIZE = 100 * 1024 # 100 kB

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

    def read(self, key: bytes) -> bytes:
        return self.db[key]

    def write(self, key: bytes, value: bytes):
        return self.db[key] = value

    def getRange(self, start: bytes, end: bytes, sort_order: int = DESC) -> dict_items:
        return self.db.items(start, end) if sort_order == ASC else self.db.items(start, end, btree.DESC)
