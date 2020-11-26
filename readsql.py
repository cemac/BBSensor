#https://www.bluehost.com/help/article/managing-databases-with-command-line-ssh
import pandas as pd
import sqlite3
import sys

'''
pip3 uninstall numpy #remove previously installed package
sudo apt install python3-numpy
'''

# Read sqlite query results into a pandas DataFrame
conn = sqlite3.connect("/root/sensor.db")
df = pd.read_sql_query("SELECT * from MEASUREMENTS", conn)


# cursor = conn.execute("SELECT * from MEASUREMENTS")
# for row in cursor:
#     print(row)

# Verify that result of SQL query is stored in the dataframe

print(df.drop('LOC',axis=1).tail(n=50))

if 'csv' in sys.argv:
  df.drop('LOC',axis=1).to_csv('/root/deleteme.csv')
  print(' Written to /root/deleteme.csv. Please Delete after')
  

#df.to_csv('./Measurements.csv', columns = ['TIME', 'PM1', 'PM3', 'PM10', 'SP', 'RC'], index=False)

conn.close()
