#!/usr/bin/env python

import os
import sys
import etcd
import subprocess
import signal
import time

if len(sys.argv) < 2:
	print("Please provide a server argument")
	sys.exit(1)

def siginthandler(signum, stackframe):
	sys.exit(-1)

signal.signal(signal.SIGINT, siginthandler)

logpath="/log"

if len(sys.argv) > 2:
	logpath=sys.argv[2]

while True:
	try:
		idx = 0
		time.sleep(1)
		p = 2379
		print("Connect to {}:{}".format(sys.argv[1], p))
		keyval = etcd.Client(host=sys.argv[1], port=p)
		while keyval:
			res = keyval.watch(logpath, index=idx, recursive=True)
			for e in res.leaves:
				if e.key == logpath:
					idx = 0
					break
				print(e.value)
				idx = e.createdIndex+1
	except Exception as e:
		print(e)

