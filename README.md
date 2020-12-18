# BBServer!
OPC R1 server scripts for Pi3

## Power Led meaning:
- Led on whist sampling
- Led off when writing to file
- blink 1s on loading code
- blink 3 seconds on upload  (sending data)


## Setup
### seting up logger
`bash setup/opc_install.sh`

#### Then run the relevant tests
`cd serverpi && python3 runtests.py`

### server
`bash setup/...install.sh`


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
- code to run measurement unit on boot
- server init




## Create a new database
`python -m serverpi.db` and type `yes`



### Debug corruption on device

```
find .git/objects/ -size 0 -exec rm -f {} \;
git fetch origin

git reset --hard origin/
```
