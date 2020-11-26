import sqlite3
from ..log_manager import getlog
log = getlog(__file__)

def builddb(conn):

    cursor = conn.cursor()

    #ID INTEGER PRIMARY KEY AUTOINCREMENT,
    conn.execute('''
                 CREATE TABLE MEASUREMENTS
                 (
                     SERIAL       CHAR(16)    NOT NULL,
                     TYPE         INT         NOT NULL,
                     TIME         CHAR(6)     NOT NULL,
                     LOC          BLOB        NOT NULL,
                     PM1          REAL        NOT NULL,
                     PM3          REAL        NOT NULL,
                     PM10         REAL        NOT NULL,
                     T            REAL        NOT NULL,
                     RH           REAL        NOT NULL,
                     SP           REAL        NOT NULL,
                     RC           INT         NOT NULL,
                     UNIXTIME     INT         NOT NULL
                     );
                 ''')

    conn.execute('''
                 CREATE TABLE PUSH
                 (
                    SERIAL       CHAR(16)    NOT NULL,
                    TIME         CHAR(6)     NOT NULL,
                    DATE         CHAR(8)     NOT NULL
                    );
                ''')


    conn.commit()

    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

    table_counter = 0
    log.info("SQL Tables available: \n===================================================\n")
    for table_item in cursor.fetchall():
        current_table = table_item[0]
        table_counter += 1
        log.info("-> " + current_table)
    log.info("\n===================================================\n")
