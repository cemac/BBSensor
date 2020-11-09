import sqlite3

conn = sqlite3.connect('test.db')

conn.execute('''
CREATE TABLE HELLOWORLD
         (ID INT PRIMARY KEY     NOT NULL,
         NAME           TEXT    NOT NULL,
         NSAT         INT     NOT NULL,
         LON        CHAR(50),
         PM1         REAL);

''')

conn.execute("INSERT INTO HELLOWORLD (ID,NAME,NSAT,LON,PM1) \
      VALUES (1, 'PiZero', 3, 'CaliforniaDreamingEncrypted', 20000.00 )");

conn.execute("INSERT INTO HELLOWORLD (ID,NAME,NSAT,LON,PM1) \
      VALUES (?,?,?,?,?)",(2, 'Pi3', 66, 'DiscoInfernoEncrypted', 111.00 ));


    # Larger example that inserts many records at a time
# purchases = [('2006-03-28', 'BUY', 'IBM', 1000, 45.00),
#              ('2006-04-05', 'BUY', 'MSFT', 1000, 72.00),
#              ('2006-04-06', 'SELL', 'IBM', 500, 53.00),
#             ]
# c.executemany('INSERT INTO stocks VALUES (?,?,?,?,?)', purchases)

    # sql = "INSERT INTO Docs(Data) VALUES (?)"
    # cur.execute(sql, (lite.Binary(data), ))


# read

cursor = conn.execute("SELECT id, name, nsat, pm1 from HELLOWORLD")
for row in cursor:
   print (row)


# cursors empties when iterated
cursor = conn.execute("SELECT id, name, nsat, pm1 from HELLOWORLD")


assert list(cursor)[0][1] == 'PiZero'

print('SQLdb PASSED')


import os
os.system('rm test.db')
