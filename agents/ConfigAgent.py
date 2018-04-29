import network
from message.Agent import Agent
from message.Message import Message, Key
from message import ACTION_WRITE

from agents import CONFIG_HOPS_PATH, SYSTEM_HOPS_PATH, CONFIG_FTP_PATH, CONFIG_TELNET_PATH
from agents import bcolors

class ConfigAgent(Agent):
    """ Config agent watches and records aggregate stats from incomming /config
        messages """
    path = "/config"

    def handler(self, msg: Message):
        if msg.path == CONFIG_HOPS_PATH and msg.action == ACTION_WRITE:
            key = Key(msg.key)
            node_id = key.device_id

            hops = self.read(SYSTEM_HOPS_PATH) or {}
            hops[node_id] = int(msg.value)

            print(bcolors.OKBLUE, self.node_id, "set system hops to:", hops, bcolors.ENDC)
            self.write_local(SYSTEM_HOPS_PATH, hops)
            print(bcolors.OKBLUE, self.read(SYSTEM_HOPS_PATH), bcolors.ENDC)

        if msg.path == CONFIG_FTP_PATH and msg.action == ACTION_WRITE:
            enable = msg.value
            status = network.ftp.status()[0]
            if enable and status == -1:
                network.ftp.start()
            elif not enable and status > -1:
                network.ftp.stop()

        if msg.path == CONFIG_TELNET_PATH and msg.action == ACTION_WRITE:
            enable = msg.value
            status = network.telnet.status()[0]
            if enable and status == -1:
                network.telnet.start()
            elif not enable and status > -1:
                network.telnet.stop()