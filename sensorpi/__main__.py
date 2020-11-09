'''
SensorPI LIBRARY

A library to run the portable sensors for the born in brandford project.

Developers:
    D.Ellis CEMAC @ UOLeeds
    C.Symonds CEMAC @ UOLeeds
PIs:
    K.Pringle UOLeeds
    J.McQuaid UOLeeds

'''
########################################################
##  Imports and constants
########################################################

import time,sys,os
from datetime import date,datetime
## runtime constants
DEBUG  = True
SERIAL = os.popen('cat /sys/firmware/devicetree/base/serial-number').read() #16 char key
DATE   = date.today().strftime("%d/%m/%Y")
STOP   = False
TYPE   = 2 # { 1 = static, 2 = dynamic, 3 = isolated_static, 4 = home/school}
LAST_SAVE = None
DHT_module = False

SAMPLE_LENGTH_slow = 60*5
SAMPLE_LENGTH_fast = 60*1 # in seconds
SAMPLE_LENGTH = SAMPLE_LENGTH_fast
# assert SAMPLE_LENGTH > 10

### hours (not inclusive)
NIGHT = [18,7] # stop 7-7
SCHOOL = [9,15] # stop 10 -2

########################################################
## Bluetooth setup
########################################################
# '''
# Start bluetooth DEBUG
# '''
# if DEBUG:
#     print('debug:', DEBUG)
#     try:
#         # Load watch command for bluetooth
#         # do this after 10 second delay from code to allow pi to finish booting.
#         bserial = True
#         os.system("screen -S ble -X stuff 'sudo rfcomm release rfcomm1 1 ^M' ")
#         os.system("screen -S ble -X stuff 'sudo rfcomm watch rfcomm1 1 & ^M' ")
#         # open('/dev/rfcomm1','w',1)
#         # bserial.write('starting')
#         # bserial.close()
#         print('debug using bluetooth serial: on')
#     except:print('no bluetooth serial')

########################################################
########################################################




########################################################
## Lib Imports
########################################################

from .tests import pyvers
# from .geolocate import lat,lon,alt
from .exitcondition import GPIO
from . import power
loading = power.blink_nonblock_inf()

from .crypt import scramble
from . import db
from .db import builddb
from . import upload
if DHT_module: from . import DHT
from . import gps
gpsdaemon = gps.init(wait=False)
from . import R1
alpha = R1.alpha

def interrupt(channel):
    print ("Pull Down on GPIO 21 detected: exiting program")
    global STOP
    STOP = True

GPIO.add_event_detect(21, GPIO.RISING, callback=interrupt, bouncetime=300)

print('########################################################')
print('starting',datetime.now())
R1.clean(alpha)

while loading.isAlive():
    if DEBUG: print('stopping loading blink ...')
    power.stopblink(loading)
    loading.join(.1)


########################################################
########################################################




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
        # now = datetime.utcnow()now.strftime("%H%M%S")
        #print(time.time()-start , SAMPLE_LENGTH)

        pm = R1.poll(alpha)

        if float(pm['PM1'])+float(pm['PM10'])  > 0:  #if there are results.

            if DHT_module: rh,temp = DHT.read()
            else:
                temp = pm['Temperature']
                rh   = pm[  'Humidity' ]

            loc     = gps.last.copy()
            unixtime = int(datetime.utcnow().strftime("%s")) # to the second

            # if DEBUG: print(rh,temp,loc)

            results.append( [SERIAL,TYPE,loc['gpstime'][:6],scramble(('%s_%s_%s'%(loc['lat'],loc['lon'],loc['alt'])).encode('utf-8')),float(pm['PM1']),float(pm['PM2.5']),float(pm['PM10']),float(temp),float(rh),float(pm['Sampling Period']),int(pm['Reject count glitch']),unixtime,] )

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
        #if DEBUG:print(d[-1])

        ''' add to db'''
        db.conn.executemany("INSERT INTO MEASUREMENTS (SERIAL,TYPE,TIME,LOC,PM1,PM3,PM10,T,RH,SP,RC,UNIXTIME) \
              VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", d );
        db.conn.commit() # dont forget to commit!

        #if DEBUG:
                # if bserial : os.system("screen -S ble -X stuff 'sudo echo \"%s\" > /dev/rfcomm1 ^M' " %'_'.join([str(i) for i in d[-1]]))

        if DEBUG: print('saveddb')

        power.ledon()

    if STOP:break


    hour = datetime.now().hour#gps.last.copy()['gpstime'][:2]


    if (hour > NIGHT[0]) or (hour < NIGHT[1]): #>18 | <7 
        ''' hometime - SLEEP '''
        if DEBUG: print('NightSleep')
        if gpsdaemon.is_alive() == True: gps.stop_event.set() #stop gps
        power.ledon()
        SAMPLE_LENGTH = -1 # Dont run !  SAMPLE_LENGTH_slow
        time.sleep(60*60) # sleep 1h
        TYPE = 4

    elif (hour > SCHOOL[0]) and (hour < SCHOOL[1]): # >7 <9 & >15 <18 utc (9-15)
        if DEBUG: print('@ School')
        ''' at school - try upload'''
        ''' rfkill block wifi; to turn it on, rfkill unblock wifi. For Bluetooth, rfkill block bluetooth and rfkill unblock bluetooth.'''
    
        DATE = date.today().strftime("%d/%m/%Y")
        
        if gpsdaemon.is_alive() == True: gps.stop_event.set() #stop gps

        if DATE != LAST_SAVE:
            if upload.online():
                #check if connected to wifi
                loading = power.blink_nonblock_inf_update()
                ## SYNC
                upload_success = upload.sync(SERIAL,db.conn)

                if upload_success:
                    cursor=db.conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                    table_list=[]
                    for table_item in cursor.fetchall():
                        table_list.append(table_item[0])

                    for table_name in table_list:
                        print ('Dropping table : '+table_name)
                        db.conn.execute('DROP TABLE IF EXISTS ' + table_name)

                    print('rebuilding db')
                    builddb.builddb(db.conn)

                    ## update time!
                    os.system('sudo timedatectl &')

                    ## run git pull
                    #################

                    #################

                    while loading.isAlive():
                        power.stopblink(loading)
                        loading.join(.1)

                    print('upload complete', DATE, hour)
                    LAST_SAVE = DATE


        SAMPLE_LENGTH = SAMPLE_LENGTH_slow

        # sleep for 18 minutes - check break statement every minute

        # check if we are trying to stop the device every minute
        for i in range(12):
            time.sleep(5*60) #12 sets of 5 min
            if STOP:break

        TYPE = 4
    else:
        ''' en route - FASTSAMPLE'''

        print (gpsdaemon.is_alive())

        if gpsdaemon.is_alive() == False:
            gpsdaemon = gps.init(wait=False)

        SAMPLE_LENGTH = SAMPLE_LENGTH_fast
        TYPE = 2


########################################################
########################################################


print('exiting- STOP:',STOP)
db.conn.commit()
db.conn.close()
power.ledon()
