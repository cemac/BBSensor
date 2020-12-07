'''
The run code for recording PM measurements using the RPI0 sensor unit.
See readme for more informationself.

Run: python3 -m sensorpi

Code: see __main__.py

Tests: python3 -m sensorpi.tests

Create NEW database: python3 -m sensorpi.db new


Individual Test: python3 -m sensorpi.tests.gps_test
'''


#
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
#
########################################################
########################################################