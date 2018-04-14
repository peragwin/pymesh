import time

def fmt_time(t: int) -> bytes:
    tt = time.localtime(t)
    return bytes('-'.join(str(v) for v in tt[:6]), 'utf-8')

def parse_time(t: bytes) -> int:
    tt = tuple(int(v) for v in t.split(b'-'))
    try:
        return time.mktime(tt[:6] + (0,0))
    except:
        return time.mktime(tt[:6] + (0,0,0))

class Key:
    time = 0
    device_id = b''
    data_id = b''

    def __init__(self, k: bytes = b'', tim=0, device_id=b'', data_id=b''):
        if k:
            t, p, d = k.split(b':')
            self.time = parse_time(t)
            self.device_id = p
            self.data_id = d
        else:
            self.time = tim
            self.device_id = device_id
            self.data_id = data_id


    def string(self):
        t = fmt_time(self.time)
        return t + b':' + self.device_id + b':' + self.data_id


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