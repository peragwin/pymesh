
from storage.Key import Key, Path
from storage.Store import Store
from storage.queue import Queue, LocalStorage

class Router:
    """ Router manages a collection of Queues. A queue is created for local storage
        as well as each newly discovered device. """

    SYSTEM_PATH = Path("system")
    DEVICES_PATH = Path("devices")

    def __init__(self, store: Store, deviceID: str):
        self.localStorage = LocalStorage(store, Key(deviceID, None, None, None))

        self.localStorage.getPath(str(self.DEVICES_PATH))
