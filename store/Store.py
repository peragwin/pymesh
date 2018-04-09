import os
import btree

from store.Data import Data
from store.Table import Table
from store.util import Key

MAX_OPEN_TABLES = 4


class StoreV2(Data):
    
    def __init__(self, store_path: str):
        self.path = store_path
        try:
            os.stat(self.path)
        except OSError:
            print("path '%s' not found; creating" % store_path)
            os.mkdir(self.path)
        
        self.data = Data(self.path + '/rawdata')

    def read(self, key: Key) -> bytes:
        return self.db[key.string()]

    def write(self, key: Key, value: bytes):
        self.db[key.string()] = value
        self.db.flush()

    def latest(self, path: str) -> tuple:
        return next( self.db.items(None, None, btree.DESC) )


class Store:
    """ A Store is a collection of tables """
    def __init__(self, store_path: str):
        self.path = store_path
        try:
            os.stat(self.path)
        except OSError:
            print("path '%s' not found; creating" % store_path)
            os.mkdir(self.path)

        self._open_tables = {}
        self._lr_opened_tables = []

    def open_table(self, pth: str) -> Table:
        print("before prepend:", pth)
        path = self.path + pth
        print("after prepend:", path)
        try:
            table = self._open_tables[path]
        except KeyError:
            table = Table(path)
            print("opened:", table.path)
        
        self._open_tables[path] = table
        self._lr_opened_tables = [path] + self._lr_opened_tables
        print(self._open_tables, self._lr_opened_tables)

        if len(self._lr_opened_tables) > MAX_OPEN_TABLES:
            path = self._lr_opened_tables.pop()
            self._close_table(path)

        return table

    def close_table(self, pth: str):
        path = self.path + pth
        self._close_table(path)
    
    def _close_table(self, path: str):
        table = self._open_tables[path]
        table.close()
        del self._open_tables[path]

    def close(self):
        for table in self._open_tables.values():
            table.close()
        self._open_tables = {}
        self._lr_opened_tables = []

    def read(self, path: str, key: bytes) -> bytes:
        table = self.open_table(path)
        return table.db[key]

    def write(self, path: str, key: bytes, value: bytes):
        table = self.open_table(path)
        table.db[key] = value
        table.db.flush()

    def latest(self, path: str) -> tuple:
        table = self.open_table(path)
        try:
            return next( table.db.items(None, None, btree.DESC) )
        except StopIteration:
            raise ValueError
