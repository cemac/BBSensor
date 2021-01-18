
# make sure the following are imported
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import pandas as pd
import numpy as np
import sqlite3
import time


'''
pip3 uninstall numpy #remove previously installed package
sudo apt install python3-numpy
'''

__KEY__ = "/Users/wolfiex/bbkey/decrypt.pem"
__DB__ = './0000000027a0b088_2020_12_19_1745'

#########      Private device only    ##########
def read_private (filename):
    with open(filename, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=None,
            backend=default_backend()
        )
    return private_key

#########      Private device only    ##########
read_data = []
private_key = read_private(__KEY__)

algorithm = padding.OAEP(
    mgf=padding.MGF1(algorithm=hashes.SHA256()),
    algorithm=hashes.SHA256(),
    label=None
)

def decrypt(encrypted):
    return private_key.decrypt( encrypted, algorithm )

def convert_to_degrees(raw_value):
    decimal_value = raw_value/100.00
    degrees = int(decimal_value)
    mm_mmmm = (decimal_value - int(decimal_value))/0.6
    position = degrees + mm_mmmm
    #position = "%.4f" %(position)
    return position

#@np.vectorize
def parse(buff):
    try:
        loc = decrypt(str(buff))
    except Exception as e:
        print(e)
        return [None,None,None]
    if loc == '__': return [np.nan,np.nan,np.nan]
    else:
        loc = loc.split('_')
        for i in [0,1]:
            loc[i] = convert_to_degrees(float(loc[i]))
        loc[2] = float(loc[2])
        return loc


#########      OPEN db       ##########

def get_data(sqlstr = "SELECT * from MEASUREMENTS" ,real=False):
    # Read sqlite query results into a pandas DataFrame

    start = time.time()

    conn = sqlite3.connect(__DB__)
    df = pd.read_sql_query(sqlstr, conn)

    print ('Extracting %d values using the SQL string: "%s"'%(len(df),sqlstr))

    df['DATE'] = pd.to_datetime(df['UNIXTIME'],unit='s')

    mid = time.time()

    ret = [parse(i) for i in df.LOC.values]
    df = pd.concat( [df.drop('LOC',axis=1)  ,pd.DataFrame(data = ret,columns='LAT LON ALT'.split())], axis =1  )

    if real: df = df[np.isnan(df.LAT)==False]

    conn.close()

    end = time.time()
    total = (end-start)/60
    parsetime = (end-mid)/60

    print('This took %.2f minutes, of which decryption was %.2f minutes'%(total,parsetime))

    return df






if __name__ == '__main__':
    df = get_data()
    
    df.to_csv('data.csv')
    print (df.columns)
    print(df['TIME PM1 PM3 PM10 SP RC DATE LAT LON'.split()].tail(n=50))
