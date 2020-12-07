import os,sys 

commit = 'AutoCommit '+' '.join(sys.argv[1:])

os.system('cd SensorMod && git add -A && git commit -m "%s" && git push'%commit)

os.system('git add -A && git commit -m "%s" && git push'%commit)

print('fi', commit)