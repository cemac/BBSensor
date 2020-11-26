'''
The logging file
'''

import logging,os

user = os.popen('echo $USER').read().strip()
if user == 'root': __RDIR__ = '/root'
else: __RDIR__ = '/home/'+user

logfile = __RDIR__+'/script.log'
print('logging in ',logfile)




console_level = logging.INFO

'''
Console Stream
'''
console = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)-10s  %(message)s')
console.setFormatter(formatter)
console.setLevel(console_level)

'''
File Debug
'''
tofile = logging.FileHandler(logfile, mode='a')
formatter = logging.Formatter('%(asctime)s ~ %(name)s ~ %(levelname)s ~ %(message)s')
tofile.setFormatter(formatter)
tofile.setLevel(logging.DEBUG)
  

  
  

def getlog(name):
  '''
  Function to set up a new logger for each module
  '''
  name = str(name)
  if name=='': name = 'unknown'
  
  log = logging.getLogger(name) ## if running interactively with ipython, replace this with a descriptive string
  log.propagate = False
  log.setLevel(logging.DEBUG)
  
  '''
  Remove any pre-existing handlers
  '''

  while len(log.handlers):
    log.removeHandler(log.handlers[0])
    
    
  '''
  Console Stream
  '''
  log.addHandler(console)

  '''
  File Debug
  '''
  log.addHandler(tofile)
  
  '''
  Make log printlike
  '''
  def print(*argv): 
    return log.info(' '.join(map(str,argv))) 
  
  log.print = print
  
  
  
  return log
