# BBStaticSensor
OPC R1 static sensor scripts for Pi3

## Power Led meaning:
- Led on whist sampling
- Led off when writing to file
- blink 1s on loading code
- blink 3 seconds on upload  (sending data)


## Setup
### seting up logger
`bash setup/opc_install.sh` - Requires internet connection

in `sudo raspi-config`, enable SPI interface

finally, run `sudo bash keygen.sh` while connected to the internet through the serverpi

#### Then run the relevant tests
`python3 -m sensorpi.tests interrupt db opc`


## link .rc_local to run on boot
Open `sudo nano /etc/rc.local`
and source the local rc file in this directory.

For testing this will likely be under `/home/pi/BBSensor/rc.local` but will eventually NEED to be changed to the `/root` folder when deployed permanently.

We add the lines:

``` bash /home/pi/BBSensor/rc.local ```
or
``` bash /root/BBSensor/rc.local ```

### rc.local contents

This contains:
- network clock updates on load (takes a number of seconds)
- update of repository
- code to run measurement unit on boot

## Create a new database
`python -m sensorpi.db` and type `yes`


### Debug corruption on device

```
find .git/objects/ -size 0 -exec rm -f {} \;
git fetch origin

git reset --hard origin/
```
