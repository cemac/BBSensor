## To test serialport for USB GPS

`python -m serial.tools.miniterm`

https://opensource.com/article/20/5/usb-port-raspberry-pi-python

## To change to using the GPIO pins, 
Change the GPIO variable in __init__ and connext to the relevant pins : tx to rx and vice versa. 

This uses an optional power pin with a transistor to switch the gps on and off. 
