from Data import Data, Key

class Table(Data):
    def __init__(self, data_path: str):
        p = data_path + '/.data'
        super().__init__(p)

        # meta_path = path_prefix + data_path + '/.meta'
        # self.meta = Store(meta_path)

        # self._children = {}
        # for key in self.meta.keys():
        #     if key.startswith('table:'):
        #         table_name = key.split(':')[1]
        #         table_path = data_path + '/' + table_name
        #         table = Table(table_path, path_prefix)
        #         self._children[table_path] = table_path

    # def type(self):
    #     return self._type

    # def isdir(self):
    #     return self.meta

    # def contains(self, path, key: bytes) -> bool:
    #     try:
    #         os.stat
    #     if ('table:' + path) in self.meta:
    #         return key in self.db
    #     if path not in self._children:
    #         return False
    #     return self._children[path].contains(path, key)

    # def get(self, path, key: bytes, value: bytes) -> bytes:
    #     next_path = path.next()
    #     if key in self.db:
    #         return self.db[key]
    #     else if ('table:' + next_path) in self.meta:
    #         return 
             

