'''
Turn off gpio pins
'''

import RPi.GPIO as GPIO

GPIO.setwarnings(False)
GPIO.setmode(GPIO.BCM)

from DHT import pin as dht_pin
from gps import pin as gps_pin
    
for pin in [dht_pin,gps_pin]:
    print('Turning off',pin)
    GPIO.setup(pin, GPIO.IN)
