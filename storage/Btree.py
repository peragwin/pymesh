
try:
    from typing import Generator, Tuple
except:
    pass

from io import TextIOWrapper

from storage import ASC, DESC, Base
from storage.Key import Key

DEFAULT_CACHE_SIZE = 128 * 1024 # 128 kB

KEY_DELIMITER = '\x1C'
START_DELIMITER = '\x00'
END_DELIMITER = '\x1F'

class Btree(Base):
    """ Data implements a datastore """

    def __init__(self, btree, stream: TextIOWrapper, cache_size=DEFAULT_CACHE_SIZE):
        self._stream = stream
        self._btree = btree

        db = btree.open(stream, cachesize=cache_size)
        self.db = db

    def __del__(self):
        self.close()

    def close(self):
        self.db.flush()
        self.db.close()
        self._stream.close()

    def _serialize_key(self, k: Key) -> bytes:
        return bytes(k.device_id, 'utf-8') + \
            KEY_DELIMITER + \
            bytes(str(k.time), 'utf-8') + \
            ((KEY_DELIMITER + bytes(k.path, 'utf-8')) if k.path else '')  + \
            ((KEY_DELIMITER + bytes(k.key, 'utf-8')) if k.key else '')

    def _deserialize_key(self, b: bytes) -> Key:
        k = b.split(KEY_DELIMITER)
        try:
            return Key(
                str(k[0], 'utf-8'),
                int(k[1]),
                str(k[2], 'utf-8'),
                str(k[3], 'utf-8'),
            )
        except:
            raise ValueError("unable to deserialize key: " + str(b, 'utf-8'))

    def read(self, key: Key) -> bytes:
        k = self._serialize_key(key)
        return self.db[k]

    def write(self, key: Key, value: bytes):
        k = self._serialize_key(key)
        self.db[k] = value

    def getRange(self, start: Key, end: Key, reverse: bool = False) \
        -> Generator[Tuple[Key, bytes], None, None]:
        
        s = self._serialize_key(start)
        e = self._serialize_key(end)
        if not reverse:
            items = self.db.items(s, e)
        else:
            items = self.db.items(s, e, self._btree.DESC)

        for k, v in items:
            yield (self._deserialize_key(k), v)

def main():
    class Mock(dict):
        DESC = 1
        def open(self, file, cache_size):
            pass
        def close(self):
            pass
        def flush(self):
            pass
    
    bt = Btree(Mock(), None)

    # test writes

    # test reads

    # test getRange

    # test close

    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())