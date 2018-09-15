
try:
    from typing import Generator, Tuple
except:
    pass

from io import IOBase
from storage import Base
from storage import PartitionAlreadyExistsError, UnsupportedIndexError
from storage.Key import Key

DEFAULT_CACHE_SIZE = 128 * 1024 # 128 kB

class PartitionKey(Exception):
    pass

BY_TIME_INDEX = b'\x01'
BY_PATH_INDEX = b'\x02'

class Btree(Base):
    """ Btree implements a datastore. The default index is BY_TIME_INDEX but the driver
        also supports BY_PATH_INDEXing. """
    
    # Partitioning can break if any element contains a byte from one of these values
    # Example:
    #   "foo" \x1D 
    #   "foo" \x1E "bar"
    #   "foo" \x1F
    #   "foo\x1Dbar" \x1E "bar" <-- breaks  
    START_PARTITION = b'\x1D'
    KEY_DELIMITER = b'\x1E'
    END_PARTITION = b'\x1F'

    BY_TIME_INDEX = BY_TIME_INDEX
    BY_PATH_INDEX = BY_PATH_INDEX

    def __init__(self, btree, stream: IOBase, cache_size=DEFAULT_CACHE_SIZE):
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

    def _serialize_time_indexed_key(self, k: Key, partitioner: chr = b'') -> bytes:
        bs = bytes(k.device_id, 'utf-8')
        if k.time is not None:
            bs += self.KEY_DELIMITER + bytes(str(k.time), 'utf-8')
        else:
            return bs + partitioner
        if k.path is not None:
            bs += self.KEY_DELIMITER + bytes(k.path, 'utf-8')
        else:
            return bs + partitioner
        if k.key is not None:
            bs += self.KEY_DELIMITER + k.key
        else:
            return bs + partitioner
        return bs

    def _deserialize_time_indexed_key(self, b: bytes) -> Key:
        k = b.split(self.KEY_DELIMITER)
        if len(k) < 4:
            if b[-1:] in [self.START_PARTITION, self.END_PARTITION]:
                raise PartitionKey
        try:
            return Key(
                str(k[0], 'utf-8'), # deviceID
                float(k[1]) if len(k) > 1 else None, # time
                str(k[2], 'utf-8') if len(k) > 2 else None, # path
                k[3] if len(k) > 3 else None, # key
            )
        except:
            raise ValueError("unable to deserialize key: " + str(b, 'utf-8'))

    def _serialize_path_indexed_key(self, k: Key, partitioner: chr = b'') -> bytes:
        bs = bytes(k.device_id, 'utf-8')
        if k.path is not None:
            bs += self.KEY_DELIMITER + bytes(str(k.path), 'utf-8')
        else:
            return bs + partitioner
        if k.key is not None:
            bs += self.KEY_DELIMITER + k.key
        else:
            return bs + partitioner
        if k.time is not None:
            bs += self.KEY_DELIMITER + bytes(str(k.time), 'utf-8')
        else:
            return bs + partitioner
        return bs

    def _deserialize_path_indexed_key(self, b: bytes) -> Key:
        k = b.split(self.KEY_DELIMITER)
        if len(k) < 4:
            if b[-1:] in [self.START_PARTITION, self.END_PARTITION]:
                raise PartitionKey
        try:
            return Key(
                str(k[0], 'utf-8'), # deviceID
                float(k[3]) if len(k) > 3 else None, # time is last
                str(k[1], 'utf-8') if len(k) > 1 else None, # path
                k[2] if len(k) > 2 else None, # key
            )
        except:
            raise ValueError("unable to deserialize key: " + str(b, 'utf-8'))

    def _serialize_key(self, k: Key, partitioner: chr = b'', index: chr = BY_TIME_INDEX) -> bytes:
        if index == self.BY_TIME_INDEX:
            return self._serialize_time_indexed_key(k, partitioner)
        elif index == self.BY_PATH_INDEX:
            return self._serialize_path_indexed_key(k, partitioner)
        else:
            raise UnsupportedIndexError

    def _deserialize_key(self, b: bytes, index: chr = BY_TIME_INDEX) -> Key:
        if index == self.BY_TIME_INDEX:
            return self._deserialize_time_indexed_key(b)
        elif index == self.BY_PATH_INDEX:
            return self._deserialize_path_indexed_key(b)
        else:
            raise UnsupportedIndexError

    def get(self, key: Key, index: chr = BY_TIME_INDEX, partitioner: chr = '') -> bytes:
        k = self._serialize_key(key, partitioner, index)
        return self.db[k]

    def store(self, key: Key, value: bytes, index: chr = BY_TIME_INDEX, partitioner: chr = ''):
        k = self._serialize_key(key, partitioner, index)
        self.db[k] = value

    def getRange(self, start: Key, end: Key, reverse: bool = False, index: chr = BY_TIME_INDEX) \
        -> Generator[Tuple[Key, bytes], None, None]:
        """ getRange returns an iterator with all key-value items between the `start` and `end` key.
            Start and end partition markers are suffixed if either key is incomplete. """
        
        s = self._serialize_key(start, self.START_PARTITION, index)
        e = self._serialize_key(end, self.END_PARTITION, index)

        if not reverse:
            items = self.db.items(s, e)
        else:
            items = self.db.items(e, s, self._btree.DESC)

        for k, v in items:
            try:
                yield (self._deserialize_key(k), v)
            except PartitionKey:
                continue

    def createPartition(self, key: Key, index: chr = BY_TIME_INDEX):
        """ createPartition for all keys that are prefixed with the given key """
        if self.hasPartition(key):
            raise PartitionAlreadyExistsError
        
        self.store(key, b'\x00', index, self.START_PARTITION)
        self.store(key, b'\x00', index, self.END_PARTITION)

    def hasPartition(self, key: Key, index: chr = BY_TIME_INDEX) -> bool:
        """ hasPartition returns true if the storage contains a partition for the given key """
        try:
            _ = self.get(key, index, self.START_PARTITION)
            return True
        except KeyError:
            return False

def main():
    class Mock(dict):
        DESC = 1
        def open(self, file, cachesize=None):
            return self
        def close(self):
            pass
        def flush(self):
            pass
        def items(self, start=None, end=None, flags=0):
            rev = flags & Mock.DESC == Mock.DESC
            items = iter(sorted(super().items(),
                key = lambda a: a[0],
                reverse = rev))
            if rev:
                start, end = end, start
            filt = lambda e: \
                (e[0] >= start if start else True) and \
                (e[0] < end if end else True)
            return filter(filt, items)

    import io
    try:
        import btree
        bt = Btree(btree, io.BytesIO())
    except (ImportError, ModuleNotFoundError):
        bt = Btree(Mock(), io.BytesIO())

    data = [
        (Key("devA", 10, "/some/path", b'col'), b"value A1"),
        (Key("devA", 11, "/some/path", b'col'), b"value A2"),
        (Key("devA", 12, "/some/path", b'col'), b"value A3"),

        (Key("devB", 10, "/some/path", b'col'), b"value B1"),
        (Key("devB", 11, "/some/path", b'col'), b"value B2"),
        (Key("devB", 12, "/some/path", b'col'), b"value B3"),
    ]

    # test writes
    for (k, v) in data:
        bt.store(k, v)

    # test reads
    for k, v in data:
        assert v == bt.get(k), "unexpected data for {0}: {1}".format(k, bt.get(k))

    # test create partitions
    partKeyA, partKeyB = Key("devA", None, None, None), Key("devB", None, None, None)
    assert bt.hasPartition(partKeyA) == False, "expected partition not to exist"

    bt.createPartition(partKeyA)
    bt.createPartition(partKeyB)

    assert bt.hasPartition(partKeyA), "expected hasPartition for devA"
    assert bt.hasPartition(partKeyB), "expected hasPartition for devB"

    # test getRange
    rng = list(bt.getRange(partKeyA, partKeyA))
    #print(rng)
    assert len(rng) == 3, "wrong number of elements from getRange"
    for i, kv in enumerate(rng):
        assert data[i] == kv, "expected kv {0}, got {1}".format(data[i], kv)
    
    ok = False
    for i, kv in enumerate(bt.getRange(partKeyB, partKeyB, True)):
        ok = True
        assert data[5-i] == kv, "expected kv {0}, got {1}".format(data[5-i], kv)
    assert ok

    # test close

    print("PASS")
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
