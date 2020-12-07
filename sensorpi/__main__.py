#!/usr/bin/env/python3
# -*- coding: utf-8 -*-

"""
SensorPI LIBRARY

A library to run the portable sensors for the born in brandford project.

Project: Born In Bradford Breathes

Usage : python3 -m sensorpi

"""

__author__ = "Dan Ellis, Christopher Symonds"
__copyright__ = "Copyright 2020, University of Leeds"
__credits__ = ["Dan Ellis", "Christopher Symonds", "Jim McQuaid", "Kirsty Pringle"]
__license__ = "MIT"
__version__ = "0.4.0"
__maintainer__ = "D. Ellis"
__email__ = "D.Ellis@leeds.ac.uk"
__status__ = "Prototype"

# Built-in/Generic Imports
import time,sys,os
from datetime import date,datetime
from re import sub


from .SensorMod import oled

oled.standby()
sys.exit()




# Check Modules
from .tests import pyvers
from .log_manager import getlog
log = getlog(__name__)
print = log.print ## replace print function with a wrapper
log.info('########################################################'.replace('#','~'))

# Exec modules
from .exitcondition import GPIO
from . import power
from .crypt import scramble
from . import db
from .db import builddb, __RDIR__
from . import upload
from . import gps
from . import R1
########################################################
##  Running Parameters
########################################################

## runtime constants
SERIAL = os.popen('cat /sys/firmware/devicetree/base/serial-number').read() #16 char key
DATE   = date.today().strftime("%d/%m/%Y")
STOP   = False
TYPE   = 2 # { 1 = static, 2 = dynamic, 3 = isolated_static, 4 = home/school}
LAST_SAVE = None
DHT_module = False
if DHT_module: from . import DHT

SAMPLE_LENGTH_slow = 60*5
SAMPLE_LENGTH_fast = 60*1 # in seconds
SAMPLE_LENGTH = SAMPLE_LENGTH_fast
# assert SAMPLE_LENGTH > 10

### hours (not inclusive)
NIGHT = [18,7] # stop 7-7
SCHOOL = [9,15] # stop 10 -2

gpsdaemon = gps.init(wait=False)
alpha = R1.alpha
loading = power.blink_nonblock_inf()



def interrupt(channel):
    log.warning("Pull Down on GPIO 21 detected: exiting program")
    global STOP
    STOP = True

GPIO.add_event_detect(21, GPIO.RISING, callback=interrupt, bouncetime=300)

log.info('########################################################')
log.info('starting {}'.format(datetime.now()))
log.info('########################################################')


R1.clean(alpha)

while loading.isAlive():
    log.debug('stopping loading blink ...')
    power.stopblink(loading)
    loading.join(.1)


########################################################
## Check for pre-existing LAST_SAVE value
########################################################

if os.path.exists(os.path.join(__RDIR__,'.uploads')):
    with open (os.path.join(__RDIR__,'.uploads'),'r') as f:
        lines = f.readlines()
    for line in lines:
        if 'LAST_SAVE = ' in line:
            LAST_SAVE = line[12:].strip()
    if LAST_SAVE == None:
        with open (os.path.join(__RDIR__,'.uploads'),'a') as f:
            f.write('LAST_SAVE = None\n')
        LAST_SAVE = 'None'
else:
    with open (os.path.join(__RDIR__,'.uploads'),'w') as f:
        f.write("LAST_SAVE = None\n")
    LAST_SAVE = 'None'

########################################################
## Main Loop
########################################################

def runcycle():
    '''
    # data = {'TIME':now.strftime("%H%M%S"),
    #         'SP':float(pm['Sampling Period']),
    #         'RC':int(pm['Reject count glitch']),
    #         'PM1':float(pm['PM1']),
    #         'PM3':float(pm['PM2.5']),
    #         'PM10':float(pm['PM10']),
    #         'LOC':scramble(('%s_%s_%s'%(lat,lon,alt)).encode('utf-8'))
    #         'UNIXTIME': int(unixtime)
    #          }
    # Date,Type, Serial

    #(SERIAL,TYPE,d["TIME"],DATE,d["LOC"],d["PM1"],d["PM3"],d["PM10"],d["SP"],d["RC"],)
    '''
    global SAMPLE_LENGTH

    results = []
    alpha.on()
    # for i in range(SAMPLE_LENGTH-1):
    start = time.time()
    while time.time()-start < SAMPLE_LENGTH:
        # now = datetime.utcnow().strftime("%H%M%S")
        #print(time.time()-start , SAMPLE_LENGTH)

        pm = R1.poll(alpha)

        if float(pm['PM1'])+float(pm['PM10'])  > 0:  #if there are results.

            if DHT_module: rh,temp = DHT.read()
            else:
                temp = pm['Temperature']
                rh   = pm[  'Humidity' ]

            loc     = gps.last.copy()
            unixtime = int(datetime.utcnow().strftime("%s")) # to the second

            results.append( [
                            SERIAL,
                            TYPE,
                            loc['gpstime'][:6],
                            scramble(('%(lat)s_%(lon)s_%(alt)s'%loc).encode('utf-8')),
                            float(pm['PM1']),
                            float(pm['PM2.5']),
                            float(pm['PM10']),
                            float(temp),
                            float(rh),
                            float(pm['Sampling Period']),
                            int(pm['Reject count glitch']),
                            unixtime,] )

        if STOP:break
        time.sleep(.1) # keep as 1

    alpha.off()
    time.sleep(1)# Let the rpi turn off the fan
    return results


########################################################
########################################################


'''
MAIN
'''

########################################################
## Run Loop
########################################################
while True:
    #update less frequenty in loop
    # DATE = date.today().strftime("%d/%m/%Y")

    if SAMPLE_LENGTH>0:
        power.ledoff()

        ## run cycle
        d = runcycle()

        ''' add to db'''
        db.conn.executemany("INSERT INTO MEASUREMENTS (SERIAL,TYPE,TIME,LOC,PM1,PM3,PM10,T,RH,SP,RC,UNIXTIME) \
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", d );
        db.conn.commit() # dont forget to commit!

        #if DEBUG:
                # if bserial : os.system("screen -S ble -X stuff 'sudo echo \"%s\" > /dev/rfcomm1 ^M' " %'_'.join([str(i) for i in d[-1]]))

        log.info('DB saved at {}'.format(datetime.utcnow().strftime("%X")))

        power.ledon()

    if STOP:break


    hour = datetime.now().hour#gps.last.copy()['gpstime'][:2]


    if (hour > NIGHT[0]) or (hour < NIGHT[1]): #>18 | <7
        ''' hometime - SLEEP '''
        log.debug('NightSleep, hour={}'.format(hour))
        if gpsdaemon.is_alive() == True: gps.stop_event.set() #stop gps
        power.ledon()
        SAMPLE_LENGTH = -1 # Dont run !  SAMPLE_LENGTH_slow
        time.sleep(30*60) # sleep 0.5h
        TYPE = 4

    elif (hour > SCHOOL[0]) and (hour < SCHOOL[1]): # >7 <9 & >15 <18 utc (9-15)
        log.debug('@ School, hour={}'.format(hour))
        ''' at school - try upload'''
        ''' rfkill block wifi; to turn it on, rfkill unblock wifi. For Bluetooth, rfkill block bluetooth and rfkill unblock bluetooth.'''

        DATE = date.today().strftime("%d/%m/%Y")

        if gpsdaemon.is_alive() == True: gps.stop_event.set() #stop gps

        log.debug('savecondition: Date = {}, Last Save = {}'.format(DATE,LAST_SAVE))
        if DATE != LAST_SAVE:
            if upload.online():
                #check if connected to wifi
                loading = power.blink_nonblock_inf_update()
                ## SYNC
                try:
                    upload_success = upload.sync(SERIAL,db.conn)
                except Exception as e:
                    log.error("Error in attempting staging upload to serverpi - {}".format(e))
                    upload_success = False
                if upload_success:
                    cursor=db.conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    table_list=[]
                    for table_item in cursor.fetchall():
                        table_list.append(table_item[0])
                    for table_name in table_list:
                        log.debug('Dropping table : '+table_name)
                        db.conn.execute('DROP TABLE IF EXISTS ' + table_name)

                    log.debug('rebuilding db')
                    builddb.builddb(db.conn)

                    log.info('upload complete %s %s'%(DATE, hour))

                    with open (os.path.join(__RDIR__,'.uploads'),'r') as f:
                        lines=f.readlines()
                    with open (os.path.join(__RDIR__,'.uploads'),'w') as f:
                        for line in lines:
                            f.write(sub(r'LAST_SAVE = '+LAST_SAVE, 'LAST_SAVE = '+DATE, line))
                    LAST_SAVE = DATE
                    log.debug('LAST_SAVE = ', LAST_SAVE)

                while loading.isAlive():
                    power.stopblink(loading)
                    loading.join(.1)

                ## update time!
                log.info(os.popen('sudo timedatectl &').read())

                ## run git pull
                log.debug('Checking git repo')
                branchname = os.popen("git rev-parse --abbrev-ref HEAD").read()[:-1]
                os.system("git fetch -q origin {}".format(branchname))
                if not (os.system("git status --branch --porcelain | grep -q behind")):
                    STOP = True

        SAMPLE_LENGTH = SAMPLE_LENGTH_slow

        # sleep for 18 minutes - check break statement every minute

        # check if we are trying to stop the device every minute
        for i in range(14):
            time.sleep(60) #5 sets of 5 min
            if STOP:break

        TYPE = 4
    else:
        log.debug('en route - FASTSAMPLE, hour={}'.format(hour))

        log.debug('GPS alive = {}'.format(gpsdaemon.is_alive()))

        if gpsdaemon.is_alive() == False:
            gpsdaemon = gps.init(wait=False)

        SAMPLE_LENGTH = SAMPLE_LENGTH_fast
        TYPE = 2


########################################################
########################################################


log.info('exiting - STOP: %s'%STOP)
db.conn.commit()
db.conn.close()
power.ledon()
if not (os.system("git status --branch --porcelain | grep -q behind")):
    now = datetime.utcnow().strftime("%F %X")
    log.critical('Updates available. We need to reboot. Shutting down at %s'%now)
    os.system("sudo reboot")
    
    
    
    
    
    
    
    
    
    
    
