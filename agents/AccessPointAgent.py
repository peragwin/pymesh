try:
    import network
except:
    import agents.test.network as network
import json

from message.Agent import Agent
from message import Broker
from message import ACTION_WRITE
from message.Message import Message

DEFAULT_PASSWORD = 'CHANGEMELATER'

class AccessPointAgent(Agent):
    """ AccessPointAgent manages the WLAN adapter in AP mode """

    path = '/system/ap'

    def __init__(self, broker: Broker):
        super().__init__(broker)

        self.ap_if = network.WLAN(network.AP_IF)
        self.ap_if.active(True)

        self.configure()

    def handler(self, msg: Message):
        if msg.action == ACTION_WRITE:
            if msg.path == self.path + '/reconfigure':
                d = msg.value
                essid = d.get('e', '')
                passwd = d.get('p', '')
                self.reconfigure(essid, passwd)


    def configure(self):
        did = str(self.node_id, 'utf-8')
        essid_path = '/config/'+did+'/ap/essid'
        self.essid = essid = self.read(essid_path)
        if not essid:
            essid = 'MESH-NODE-'+did
            self.write_local(essid_path, essid)
        
        passwd_path = '/config/'+did+'/ap/passwd'
        self.passwd = passwd = self.read(passwd_path)
        if not passwd:
            passwd = DEFAULT_PASSWORD
            self.write_broadcast(passwd_path, passwd)

        self.ap_if.config(essid=bytes(essid, 'utf-8'), passwd=bytes(passwd, 'utf-8'))

    def reconfigure(self, essid: str, passwd: str):
        essid = essid or self.essid
        passwd = passwd or self.passwd
        self.ap_if.config(essid=bytes(essid, 'utf-8'), passwd=bytes(passwd, 'utf-8'))
