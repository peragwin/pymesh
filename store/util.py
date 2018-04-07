import time

def fmt_time(t: int) -> bytes:
    tt = time.localtime(t)
    return b'-'.join(tt[:6])

def parse_time(t: bytes) -> int:
    tt = tuple(int(v) for v in t.split('-'))
    return time.mktime(t[:6])

class Key:
    time = 0
    device_id = b''
    data_id = b''

    def __init__(self, k: bytes):
        t, p, i = k.split(b':')
        self.time = parse_time(t)
        self.device_id = p
        self.data_id = i

    def string(self):
        t = time.localtime(self.time)
        return t.join('-')


class Path:
    """ Path is a generator meant to traverse a Table path """
    def __init__(self, path: bytes):
        self.__path = path
        self._path = b''
        def gen():
            for p in path.split(b'/'):
                yield p
        self._gen = gen() 
    def __next__(self):
        p = next(self._gen)
        self._path += b'/' + p
        return self._path