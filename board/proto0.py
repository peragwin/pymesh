from machine import SPI, I2C, Pin, RTC, deepsleep, DEEPSLEEP
import sdcard
import os
from sensors.bme280 import BME280

class ProtoBoard:
    version = '0.1'

    def __init__(self):
        self.spi = spi = SPI(1, baudrate=25000000, sck=Pin(14), miso=Pin(27), mosi=Pin(13))
        self.sd = sd = sdcard.SDCard(spi=spi, cs=Pin(15))
        os.mount(sd, '/sd')

        self.i2c = i2c = I2C(sda=Pin(23), scl=Pin(22))
        self.bme = BME280(i2c=i2c)

        self.sleep_wake_pin = p = Pin(34, mode=Pin.IN)
        p.irq(trigger=Pin.IRQ_FALLING, wake=DEEPSLEEP, handler=self.sleep_handler)
    
        # self.rtc = RTC()
        # self.rtc.wake_on_ext0(34, 0)

        self.led = Pin(2, mode=Pin.OUT)
        self.led.value(1)

    def sleep_handler(self, *args):
        deepsleep(0x7FFFFFF)
