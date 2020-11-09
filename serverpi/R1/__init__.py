import spidev
import opc
from time import sleep

spi = spidev.SpiDev()
spi.open(0, 0)
spi.mode = 1
spi.max_speed_hz = 20000 #500000

keep='Temperature,Humidity,Sampling Period,PM1,PM2.5,PM10'.split(',')

sleep(1.0)
try:
	alpha = opc.OPCR1(spi)
except Exception as e:
	print ("Startup Error: {}".format(e))
print (alpha)


def poll(alpha):
	return alpha.histogram()#alpha.pm()
    #return dict((k, res[k]) for k in keep)


def info(alpha):
	alpha.on()
	sleep(2)
	alpha.off()

	print("Read info string")
	print(alpha.read_info_string())

	print("Read serial number")
	print(alpha.sn())

	print("Read firmware")
	print(alpha.read_firmware())

	print("Read config")
	print(alpha.read_config())

	# print("Read Histogram")
	# print(alpha.histogram())
	# print("Read PM")
	# print(alpha.pm())


def clean(alpha):
	alpha.on()
	sleep(9)
	alpha.off()
	sleep(1)
