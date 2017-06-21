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

p = 2379
keyval = etcd.Client(host=sys.argv[1], port=p)
if len(sys.argv) >= 3:
	kv = keyval.read("/config/"+sys.argv[2])
	if kv:
		if len(sys.argv) >= 4:
			keyval.write(kv.key, sys.argv[3])
			kv = keyval.read("/config/"+sys.argv[2])
		print("{} = {}".format(kv.key.split('/')[-1], kv.value))

else:
	for kv in keyval.read("/config", recursive=True).leaves:
		print("{:30s} = {}".format(kv.key.split('/')[-1], kv.value))
