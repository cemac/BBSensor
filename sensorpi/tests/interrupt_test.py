import RPi.GPIO as GPIO
import time

'''
test script for hardware interrupt
Connect the bottom right pin (GPIO 21) to ground (bottom left) and then release

'''



# for GPIO numbering, choose BCM
GPIO.setmode(GPIO.BCM)
# or, for pin numbering, choose BOARD
#GPIO.setmode(GPIO.BOARD)
GPIO.setup(21, GPIO.IN, pull_up_down=GPIO.PUD_UP)

def interrupt(channel):
    print ("Pull Down on GPIO 21 detected: exiting program"  )
    global stop
    stop = True

global stop 
stop = False

GPIO.add_event_detect(21, GPIO.RISING, callback=interrupt, bouncetime=300)

print('runnint infinite loop until button press (GPIO 21 pull down to ground) or ctrl+c')
try:
	while (1==1):
		time.sleep(1)
		if stop: 
			print('break command  found')
			break
except KeyboardInterrupt:# on control c
        GPIO.cleanup()
        print('keyboard - not a fair test \n TEST FAILED')
