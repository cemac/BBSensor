


from datetime import datetime
import time,re,sys



###########################
## Loading
###########################


from .. import gps

loc = gps.init()

print('############# BEGIN ############')

###########################
## Functions
###########################



def fastsample():
    start = datetime.utcnow()
    location = gps.last.copy()
    location['nonGPSutc']= str(start)
    print(location)

###########################
## RunScript
###########################




print('start')

for i in range(350):
    fastsample()
    time.sleep(2)
    
    
gps.pinoff()