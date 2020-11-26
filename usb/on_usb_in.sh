#!/bin/sh

LOCK=/tmp/lockfile_for_plug_usb

if [ -f $LOCK ]
then
        exit 1
else
        touch $LOCK;
        # the actual command to run upon USB plug in
        echo '\n\n' >> /root/usb.log;
        /bin/date >> /root/usb.log;
        /usr/bin/python3 /root/BBSensor/usb/datatransfer.py >> /root/usb.log;
fi

