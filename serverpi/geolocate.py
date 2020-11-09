import os
alt=0
try:
	lat,lon = [float(i) for i in  os.popen('curl ipinfo.io/loc').read().split(',')]
except:
	lat = -999
	lon = -999


if __name__ == '__main__':
	print(lat,lon)
