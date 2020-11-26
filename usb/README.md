# The alternative data transfer mode. 

## Instructions
1. Log On as superuser
2. run a git pull
3. complete 'set up new usb' instructions below
4. Reboot - `sudo reboot`


## Setup new usb
1. Plug in USB
2. run `python3 setup_USB.py`
3. copy UUID code
4. place in `BBSensor/usb/approved.dev` on github repo to be disemminated to all devices. No transfers will happen until this is updated.

## File location
Files are transfered into the `/transferdata` directory on your USB.

## Authentication
Checksum that the encryption file on the usb matches that on the device
Only transfers if the UUID is part of the approved files list
The correct path must exist

## Program
Checks attached devices, 
mounts them and checks authentication. 
If this passes it copies the files 

## Leds- THESE ARE OVERWRITTEN BY OTHER PROGRAMS - so cannot be trusted. 
I suggest trying other files and gauging the transfer time: 

### Delays
10 seconds before any transfer happens 

## hypothetical leds 
Constant. If dark - it may be writing. If lit - likely ok.  
- Off whilst writing
- On when finished

### Logfile
`/root/usb.log`

### Bootup setup
This has been added to rc.local.
Move `optional_usb.rules` into `/etc/udev/rules.d/`


### Remove 
`rm /etc/udev/rules.d/optional_usb.rules` 
and comment out in rc.local

