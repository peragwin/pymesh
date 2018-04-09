import uhashlib
import time
import network

from message.Message import new_message, ACTION_WRITE, DEST_BROADCAST, DEST_LOCAL
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

        self.ap_if = ap = network.WLAN(network.AP_IF)
        self.sta_if = sta = network.WLAN(network.STA_IF)
        ap.active(True)
        sta.active(True)

        from ubinascii import hexlify
        self.device_id = hexlify(ap.config('mac'))

        self.configure_ap()
        self.configure_sta()

        if sta.isconnected():
            # Initialize RTC to the current time
            from ntptime import settime
            settime()
            self.rtc_synced = True
        else:
            self.rtc_synced = False
            print("running in limited mode until RTC is set")

    def configure_ap(self):
        did = str(self.device_id, 'utf-8')
        # Set up AP mode
        try:
            essid_path = path('/config', did, 'ap', 'essid')
            _, essid = self.store.latest(essid_path)
        except ValueError:
            essid = b'MESH-NODE-' + self.device_id
            self.write_broadcast(essid_path, essid)

        try:
            # First check if there is a global value set for all nodes
            _, passwd = self.store.latest(path('/config', 'ap', 'passwd'))
        except ValueError:
            # Otherwise check if there is a value stored for this node
            try:
                passwd_path = path('/config', did, 'ap', 'passwd')
                _, passwd = self.store.latest(passwd_path)
            except ValueError:
                # Finally set a default password and record it.
                passwd = b'CHANGEMELATER'
                self.write_broadcast(passwd_path, passwd)

        # TODO: set up ifconfig

        self.ap_if.config(essid=essid, password=passwd)

    def configure_sta(self, force_scan=False):
        # Set up STA mode
        sta_essid, sta_passwd = '', ''
        essid_path = path('/local', 'config', 'sta', 'essid')
        passwd_path = path('/local', 'config', 'sta', 'passwd')

        if not force_scan:
            try:   
                _, sta_essid = self.store.latest(essid_path)
            except:
                pass

            try:
                _, sta_passwd = self.store.latest(passwd_path)
            except:
                pass

        if sta_essid and sta_passwd:
            self.sta_if.connect(sta_essid, sta_passwd)
            timeout = 0
            while not self.sta_if.isconnected() and timeout < 10:
                time.sleep_ms(1000)
                timeout += 1

        else:
            # scan and sort by rssi
            scan = self.sta_if.scan().sort(key=lambda s: -s[3])
            for s in scan:
                bssid = s[0]
                if bssid.startswith(b'MESH-NODE'):
                    node_id = str(bssid.split(b'-')[-1], 'utf-8')
                    try:
                        _, passwd = self.store.latest(path('/config', node_id, 'ap', 'passwd'))
                        self.sta_if.connect(bssid, passwd)
                        timeout = 0
                        for _ in range(10):
                            time.sleep_ms(1000)
                            if self.sta_if.isconnected():
                                break
                        else:
                            continue

                        # Successfully connected, so put the config into storage
                        self.write_local(essid_path, bssid)
                        self.write_local(passwd_path, passwd)
                        break

                    except ValueError: # Config for AP not in storage
                        pass


    def write_local(self, path: str, value: bytes):
        msg = new_message(path, self.device_id, value, ACTION_WRITE, DEST_LOCAL)
        self.store.record(msg)

    def write_broadcast(self, path: str, value: bytes):
        msg = new_message(path, self.device_id, value, ACTION_WRITE, DEST_BROADCAST)
        self.store.record(msg)

