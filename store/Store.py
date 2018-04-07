from os import path
import btree

from Table import Table

MAX_OPEN_TABLES = 8

class Store:
    """ A Store is a collection of tables """
    def __init__(self, store_path: str):
        self.path = store_path
        try:
            assert path.isdir(self.path)
        except OSError:
            print("path '%s' not found" % store_path)
            raise

        self._open_tables = {}
        self._lr_opened_tables = []

    def open_table(self, path: str) -> Table:
        path = self.path + path
        try:
            table = self._open_tables[path]
        except KeyError:
            table = Table(path)
        
        self._open_tables[path] = table
        self._lr_opened_tables = [path] + self._lr_opened_tables

        if len(self._lr_opened_tables) > MAX_OPEN_TABLES:
            path = self._lr_opened_tables.pop()
            self.close_table(path)

        return table

    def close_table(self, path: str):
        path = self.path + path
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
        if key in table.db:
            return table.db[key]

    def write(self, path: str, key: bytes, value: bytes):
        table = self.open_table(path)
        table.db[key] = value

    def latest(self, path: str) -> tuple:
        table = self.open_table(path)
        return next( table.db.items(None, None, btree.DESC) )
