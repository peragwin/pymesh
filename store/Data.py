import btree
import time


class Data:
    """ Data implements a datastore """

    def __init__(self, store_path: str):
        try:
            f = open(store_path, "r+b")
        except OSError:
            f = open(store_path, "w+b")
        self._file = f
        self._path = store_path

        db = btree.open(f)
        self.db = db

    def close(self):
        self.db.flush()
        self.db.close()
        self._file.close()

    def store(self, key: bytes, value: bytes):
        self.db[key] = value

    def load(self, key: bytes) -> bytes:
        return self.db[key]

    def delete(self, key: bytes):
        del self.db[key]

    def path(self) -> bytes:
        return self._path

