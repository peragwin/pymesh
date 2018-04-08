import uhashlib
import time
import network

from message.Message import new_message, ACTION_WRITE
from message.Recorder import MessageRecorder
from store.Store import Store
from store.util import Key

STORE_PATH = '/data'

# A default of one day may not be enough since it could take more than a single day
# to notice and fix a network partition. Hopefully we can make storage nodes cheap.
DEFAULT_MESSAGE_EXPIRY = 5*24*3600

def path(*args) -> str:
    return '/'.join(args)

class Node:
    """ Node represents a node of the network """
    def __init__(self):

        self.store = MessageRecorder(STORE_PATH, DEFAULT_MESSAGE_EXPIRY)
        self.local = Store(STORE_PATH + '/local')

        ap = network.WLAN(network.AP_IF)
        ap.active(True)

        from ubinascii import hexlify
        self.device_id = hexlify(ap.config('mac'))
        did = str(self.device_id, 'utf-8')

        # Set up AP mode
        try:
            essid_path = path('/config', did, 'ap', 'essid')
            _, essid = self.store.latest(essid_path)
        except:
            essid = b'MESH-NODE-' + self.device_id[4:]
            self.store.record(new_message(essid_path, self.device_id, essid, ACTION_WRITE))

        try:
            # First check if there is a global value set for all nodes
            _, passwd = self.store.latest(path('/config', 'ap', 'passwd'))
        except:
            # Otherwise check if there is a value stored for this node
            try:
                passwd_path = path('/config', did, 'ap', 'passwd')
                _, passwd = self.store.latest(passwd_path)
            except:
                # Finally set a default password and record it.
                passwd = b'CHANGEMELATER'
                self.store.record(new_message(passwd_path, self.device_id, passwd, ACTION_WRITE))

        # TODO: set up ifconfig

        ap.config(essid=essid, password=passwd)

        # Set up STA mode
        sta_essid, sta_passwd = '', ''
        try:
            essid_path = path('/config', 'sta', 'essid')
            _, sta_essid = self.local.latest(essid_path)
        except:
            pass

        try:
            passwd_path = path('/config', 'sta', 'passwd')
            _, sta_passwd = self.local.latest(passwd_path)
        except:
            pass

        self.local.close()

        sta = network.WLAN(network.STA_IF)
        sta.active(True)
        if sta_essid and sta_passwd:
            sta.connect(sta_essid, sta_passwd)
            timeout = 0
            while not sta.isconnected() and timeout < 10:
                time.sleep_ms(1000)
                timeout += 1

        if sta.isconnected():
            # Initialize RTC to the current time
            from ntptime import settime
            settime()
            self.rtc_synced = True
        else:
            self.rtc_synced = False
            print("running in limited mode until RTC is set")

    def write_local(self, path: str, value: bytes):
        msg = new_message(path, self.device_id, value, ACTION_WRITE)
        self.local.write(msg.path, msg.key.string(), msg.value)
        self.local.close()

    def write(self, path: str, value: bytes):
        msg = new_message(path, self.device_id, value, ACTION_WRITE)
        self.store.record(msg)


