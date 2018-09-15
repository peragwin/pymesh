import time

class Key:
    """ Key is used to represent keys in storage """

    def __init__(self, device_id: str, time: float, path: str, key: bytes):
        self.device_id = device_id
        self.time = float(time) if time is not None else None
        self.path = path
        self.key = key

    def __str__(self) -> str:
        return "<Key\"{0}:{1}:{2}:{3}\">".format(self.device_id, self.time, self.path, self.key)

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return (self.device_id == other.device_id and
            self.time == other.time and
            self.path == other.path and
            self.key == other.key)

def newKey(device_id: str, path: str, key: bytes):
    return Key(device_id, time.time(), path, key)


class Path:
    """ A Path is a list of string path elements """

    def __init__(self, *path_elems: str):
        self.elems = path_elems

    def __str__(self):
        return "/" + "/".join(self.elems)

    def __add__(self, other: 'Path') -> 'Path':
        return Path(*(self.elems + other.elems))