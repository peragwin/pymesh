import network
import json
from message.Subscriber import Subscriber

class StationAgent(Subscriber):
    """ StationAgent controls the operation of the WLAN in STA mode """
    def __init__(self):
        super().__init__('/system/sta', self.message_handler)

        self.sta_if = network.WLAN(network.STA_IF)

    def start(self):
        self.configure_sta()

    def configure_sta(self, force_scan=False):
        # Set up STA mode
        sta_essid, sta_passwd = '', ''
        essid_path = path('/local', 'config', 'sta', 'essid')
        passwd_path = path('/local', 'config', 'sta', 'passwd')

        if not force_scan:
            sta_essid = self.read(essid_path)
            sta_passwd = self.read(passwd_path)

        if sta_essid and sta_passwd:
            self.sta_if.connect(sta_essid, sta_passwd)
            timeout = 0
            while not self.sta_if.isconnected() and timeout < 10:
                time.sleep_ms(1000)
                timeout += 1

        else:
            # scan and sort by rssi
            scan = self.sta_if.scan()
            scan.sort(key=lambda s: -s[3])
            for s in scan:
                bssid = s[0]
                if bssid.startswith(b'MESH-NODE'):
                    did = bssid.split(b'-')[-1]
                    node_id = str(did, 'utf-8')
                    if did == self.device_id:
                        continue
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
                        print("missing password for AP", bssid)
                        pass