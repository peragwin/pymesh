import time

def fmt_time(t: int) -> str:
    tt = time.localtime(t)[:6] + ('%d' % (1000*(t % 1)),)
    return '-'.join(str(v) for v in tt)

def parse_time(t: str) -> int:
    tt = tuple(int(v) for v in t.split('-'))
    try:
        return time.mktime(tt[:6] + (0,0)) + (tt[6] / 1000)
    except:
        return time.mktime(tt[:6] + (0,0,0)) + (tt[6] / 1000)

class Key:
    """ Key is used to represent keys in storage """

    def __init__(self, device_id: str, time: int, path: str, key: str):
        # if k:
        #     if isinstance(k, bytes):
        #         k = str(k, 'utf-8')
        #     t, p, d = k.split(':')
        #     self.time = parse_time(t)
        #     self.device_id = str(p, 'utf-8')
        #     self.data_id = str(d, 'utf-8')
        # else:
        self.device_id = device_id
        self.time = time
        self.path = path
        self.key = key


    # def string(self):
    #     t = fmt_time(self.time)
    #     return ':'.join((t, self.device_id, self.data_id))


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