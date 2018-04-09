import os
from store.Data import Data

def traverse(path: str):
    p = ''
    for elm in path.split('/'):
        p += '/' + elm
        yield p

class Table(Data):
    def __init__(self, data_path: str, **kwargs):
        p = data_path + '/.data'

        for path in traverse(data_path):
            try:
                os.stat(path)
            except:
                print("path '%s' not found; creating" % path)
                os.mkdir(path)

        super().__init__(p, **kwargs)

