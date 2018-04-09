import os
from store.Data import Data

def traverse(path: str):
    p = ''
    for elm in path.split('/'):
        p += '/' + elm
        yield p

class Table(Data):
    def __init__(self, data_path: str, mode='r', **kwargs):
        p = data_path + '/.data'

        for path in traverse(data_path):
            try:
                os.stat(path)
            except:
                if mode == 'w':
                    print("path '%s' not found; creating" % path)
                    os.mkdir(path)
                else:
                    raise ValueError('table not found')

        super().__init__(p, **kwargs)

