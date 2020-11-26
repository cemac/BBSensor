'''

Creates a directory containing encryption key on plugged usb
Returns UUID which must be added to the approved.dev file on the device

'''
import os,re,glob
PEM = glob.glob('/root/BBSensor/s*pi/crypt/encrypt.pem')[0]


usbs = []

for u in os.popen('sudo blkid').readlines():
    loc = [u.split(':')[0]]
    if '/dev/sd' not in loc[0]: continue
    loc+=re.findall(r'"[^"]+"',u)
    columns = ['loc']+re.findall(r'\b(\w+)=',u)
    
    usbs.append(dict(zip(columns,loc)))
    
    
    
for u in usbs:
    
    print ('Converting %(LABEL)s for use'%u)

    os.system('sudo umount /media')
    os.system('sudo mount %(loc)s /media'%u)
    
    if not os.path.exists('/media/transferdata'): 
        try:
            os.mkdir('/media/transferdata')
        except OSError: print ("Creation of the directory transferdata failed")
    else: print('Folder already exists')
    
    os.system('cp %s /media/transferdata/'%PEM)
    
    os.system('sudo umount /media')
    
    print ('Transfer complete.\n')
    print ('You MUST add the code below to the file in BBSensor/usb/approved.dev \n- Include the quotation marks (")\n\n')
    print(u['UUID'])
    print ('')


