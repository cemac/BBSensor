import sqlite3,os

filename = '/server.db'

# if we are root, write to root dir
user = os.popen('echo $USER').read().strip()


if user == 'root': __RDIR__ = '/root'
else: __RDIR__ = '/home/'+user

conn = sqlite3.connect(__RDIR__+filename)
