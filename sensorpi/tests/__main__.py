''' 
python3 -m sensorpi.tests
'''

import os,sys
print('This is pi of serial number: ',os.popen('cat /sys/firmware/devicetree/base/serial-number').read())


args = sys.argv[1:]
if len(args) == 0 : args = 'interrupt db temp opc gps oled'.split()


from . import pyvers
if 'interrupt' in args:
  from . import interrupt_test
if 'db' in args:
  from . import db_test
  # 
if 'temp' in args:
  from . import DHT_test
if 'opc' in args:
  from . import opc_test
if 'gps' in args:
  from . import gps_test
if 'oled' in args:
  from . import oled_test





# from . import tests.ctrlc_test
