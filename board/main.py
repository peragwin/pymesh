#from board.proto0 import ProtoBoard
#pb = ProtoBoard()

import network
import machine
import os
import time

os.sdconfig(os.SDMODE_1LINE)
os.mountsd()

sta = network.WLAN()
sta.active(True)
sta.connect('dlink-AF94', 'aipwz24505')

connected = False
for i in range(10):
    if sta.isconnected():
        connected = True
        break
    time.sleep(1)

if connected:
    network.ftp.start(user='micro', password='python')
    network.telnet.start()
    rtc = machine.RTC()
    rtc.ntp_sync(server='pool.ntp.org')
    machine.Pin(21, machine.Pin.OUT).value(1)
