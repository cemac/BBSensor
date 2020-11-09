''' 
Ensure we can do a controlled exit when pressing ctrl+c 
'''


### Failure on exit params
import sys

class ExitHooks(object):
    def __init__(self):
        self.exit_code = None
        self.exception = None

    def hook(self):
        self._orig_exit = sys.exit
        sys.exit = self.exit
        sys.excepthook = self.exc_handler

    def exit(self, code=0):
        self.exit_code = code
        self._orig_exit(code)

    def exc_handler(self, exc_type, exc, *args):
        self.exception = exc

hooks = ExitHooks()
hooks.hook()




def onexit():

    import os
    if hooks.exit_code is not None:
        print("death by sys.exit(%d)" % hooks.exit_code)
    elif hooks.exception is not None:
        print("death by exception: %s" % hooks.exception)
    else:
        print("natural death")
        
        
    print('Exiting in a controlled Manner \n Exit Condition PASSED')

  
import atexit
atexit.register(onexit)


import time

print('Infinite loop, please exit with ctrl + c') 
for i in  range(600):
	time.sleep(1)

print('Exit Condition FAILED')
