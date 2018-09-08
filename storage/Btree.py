
try:
    from typing import Generator, Tuple
except:
    pass

from io import TextIOWrapper

from storage import Base, PartitionAlreadyExistsError
from storage.Key import Key

DEFAULT_CACHE_SIZE = 128 * 1024 # 128 kB

KEY_DELIMITER = '\x1C'
START_PARTITION = '\x00'
END_PARTITION = '\x1F'

BY_TIME_INDEX = '\x01'
BY_PATH_INDEX = '\x02'

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

    # needs some kind of <deviceID>:"BYPATHINDEX":<path>:<key>:<timestamp> key format
    # but it also cant interfere with the queue reading off the latest offset and getRange
    # maybe works if we have <deviceID>:"BYTIMEINDEX":<path>:<key> format as well

    def _serialize_key(self, k: Key, partitioner: chr = '') -> bytes:
        bs = bytes(k.device_id, 'utf-8')
        if k.time is not None:
            bs += KEY_DELIMITER + bytes(str(k.time), 'utf-8')
        else:
            return bs + partitioner
        if k.path is not None:
            bs += KEY_DELIMITER + bytes(k.path, 'utf-8')
        else:
            return bs + partitioner
        if k.key is not None:
            bs += KEY_DELIMITER + bytes(k.key, 'utf-8')
        else:
            return bs + partitioner
        return bs

    def _deserialize_key(self, b: bytes) -> Key:
        k = b.split(KEY_DELIMITER)
        try:
            return Key(
                str(k[0], 'utf-8'),
                int(k[1]) if len(k) > 1 else None,
                str(k[2], 'utf-8') if len(k) > 2 else None,
                str(k[3], 'utf-8') if len(k) > 3 else None,
            )
        except:
            raise ValueError("unable to deserialize key: " + str(b, 'utf-8'))

    def get(self, key: Key, partitioner: chr = '') -> bytes:
        k = self._serialize_key(key, partitioner)
        return self.db[k]

    def store(self, key: Key, value: bytes, partitioner: chr = ''):
        k = self._serialize_key(key, partitioner)
        self.db[k] = value

    def getRange(self, start: Key, end: Key, reverse: bool = False) \
        -> Generator[Tuple[Key, bytes], None, None]:
        """ getRange returns an iterator with all key-value items between the `start` and `end` key.
            Start and end partition markers are suffixed if either key is incomplete. """
        
        s = self._serialize_key(start, START_PARTITION)
        e = self._serialize_key(end, END_PARTITION)
        if not reverse:
            items = self.db.items(s, e)
        else:
            items = self.db.items(s, e, self._btree.DESC)

        for k, v in items:
            yield (self._deserialize_key(k), v)

    def createPartition(self, key: Key):
        """ createPartition for all keys that are prefixed with the given key """
        if self.hasPartition(key):
            raise PartitionAlreadyExistsError
        
        self.store(key, b'\x00', START_PARTITION)
        self.store(key, b'\x00', END_PARTITION)

    def hasPartition(self, key: Key) -> bool:
        """ hasPartition returns true if the storage contains a partition for the given key """
        try:
            _ = self.get(key, START_PARTITION)
            return True
        except KeyError:
            return False

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