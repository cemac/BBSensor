'''
scripts to run if connected online
'''
import os

def online():

    cmd = '''
    PINGS=2
    TESTIP=8.8.8.8
    if ( ping -c $PINGS $TESTIP > /dev/null ) then
        echo "1"
    else
        echo "0"
    fi
    '''

    return int(os.popen(cmd).read())


def stage(SERIAL,conn):

    from shutil import copy2
    from datetime import datetime, date

    DATE = date.today().strftime("%d%m%Y")
    TIME = datetime.utcnow().strftime("%H%M%S")

    data = [(SERIAL,TIME,DATE,)]

    conn.executemany("INSERT INTO PUSH (SERIAL,TIME,DATE) VALUES(?, ?, ?);", data )

    conn.commit()

    filename = 'server.db'

    # if we are root, write to root dir
    user = os.popen('echo $USER').read().strip()

    if user == 'root': __RDIR__ = '/root'
    else: __RDIR__ = '/home/'+user

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    uploadfile = '.'.join(filename.split('.')[:-1])+timestamp+'.'+filename.split('.')[-1]

    try:
        copy2(os.path.join(__RDIR__,filename),os.path.join('/home/serverpi/datastaging',uploadfile))
    except:
        print ('Error copying server.db file to staging area')
        return False

    return True

def sync():

    from .sqlMerge import sqlMerge
    from glob import glob

    merge=sqlMerge()

    dataloc = '/home/serverpi/datastaging'

    sensorfiles = glob(os.path.join(dataloc,'sensor*.db'))

    serverfiles = glob(os.path.join(dataloc,'server*.db'))

    if len (serverfiles) > 1:
        for file in serverfiles[1:]:
            sensorfiles.append(file)
    elif len (serverfiles) < 1:
        print ("Could not find server.db file for merge")
        return False

    print (serverfiles[0])
    print (sensorfiles)

    #Merge the various DB and upload to AWS
    merge.mergelist(serverfiles[0], sensorfiles)

    return True
