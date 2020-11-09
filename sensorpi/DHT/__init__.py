'''
Left rail from the top

4th - output
5th - gnd
6th - vin

'''

import RPi.GPIO as GPIO
import Adafruit_DHT
import time

pin = 17 # BCM 11 / GPIO 17
DHT_PIN = 4
GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)

def pinon():
    GPIO.setup(pin, GPIO.OUT)
    GPIO.output(pin,GPIO.HIGH)
    time.sleep(1)
def pinoff():
    GPIO.setup(pin, GPIO.IN)
    time.sleep(1)

pinoff()
pinon()


DHT_SENSOR = Adafruit_DHT.DHT11 #DHT22

def read():
    # humidity, temperature =
    return Adafruit_DHT.read_retry(DHT_SENSOR, DHT_PIN)
