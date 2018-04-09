import btree
import time

CACHE_SIZE = 1024

class Data:
    """ Data implements a datastore """

    def __init__(self, store_path: str):
        try:
            f = open(store_path, "r+b")
        except OSError:
            f = open(store_path, "w+b")
        self._file = f
        self._path = store_path

        db = btree.open(f, cachesize=CACHE_SIZE)
        self.db = db

    def close(self):
        self.db.flush()
        self.db.close()
        self._file.close()
