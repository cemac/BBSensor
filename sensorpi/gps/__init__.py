'''
Get GPS values from usb


If using usb set gpio = False



screen /dev/ttyS0 9600
'''

import RPi.GPIO as GPIO
from threading import Thread,Lock,Event
lock = Lock()
import serial,time

last = {'gpstime':None}
ser = None
gpio = False#True
print('GPIO GPS:',gpio)
pin = 23#18 # BCM 12 / GPIO 18

stop_event = Event()

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
if gpio: pinon()



__all__ = 'last ser gpio connect bg_poll latlon'.split()

def connect():
    global ser,gpio
    if gpio:
        ser = serial.Serial('/dev/ttyS0')#,9600)
        print('GPIO GPS on /dev/ttyS0')
    else:
        for i in range(10):
            try:# each unplug registers as a new number, we dont expect more than 10 unplugs without a restart
                #try:ser = serial.Serial('/dev/ttyAMA%d'%i)
                #except:
                ser = serial.Serial('/dev/ttyACM%d'%i)

                print( 'Connected serial on /dev/ttyACM%d'%i)
                break
            except:continue
    ser.flushInput()
    ser.flushOutput()


def bg_poll(ser,lock,stop_event):
        global last,stopThread
        #ignored values to be named utc
        params = 'utc gpstime lat utc lon utc fix nsat HDOP alt utc WGS84 utc lastDGPS utc utc'.split()
        while not stop_event.is_set():
                try:
                    line = str(ser.readline())

                except serial.SerialException:
                    print('lost connection - reconnecting')
                    #last = {}
                    connect()
                    time.sleep(5)
                    continue

                if line.find('GGA') > 0:
                    lock.acquire()
                    last = dict(zip(params,line.split(',')))
                    lock.release()
                    #print(last)



def latlon():
    global last
    if last['lat']=='' or last['lon']=='':
        return [0,0]
    else:
        return [float(last['lat']),float(last['lon'])]


def init(wait = False):


    connect()
    print('############# GPS daemon ############')
    stop_event.clear()

    daemon = Thread(target=bg_poll, args=(ser,lock,stop_event), name='location_daemon')
    daemon.setDaemon(True)#bg
    daemon.start()

    if wait:
        while last == {'gpstime':None}:
            print('waiting for gps result')
            time.sleep(4)

        while last['gpstime'] == '':
            print(last)
            print('GPS Connected, but not reading a result - please check volatge')
            time.sleep(4)

        while len(last['gpstime']) !=6 :
            print(last)
            print('GPS Connected, but not correct')
            time.sleep(1)



    print('GPS connected')
    print(last)
    return daemon
