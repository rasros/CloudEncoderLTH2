#!/usr/bin/env python

from keyval import KeyValueStore
import threading
import os

class NotifyThread(threading.Thread):
	def __init__(self, name=None, host=None):
		threading.Thread.__init__(self)
		self.stopped = threading.Event()
		if host is None:
			self.keyval = KeyValueStore(port=2379)
		else:
			self.keyval = KeyValueStore(host=host, port=2379)
		if name is None:
			self.name = os.uname()[1]
		else:
			self.name = name

	def stop(self):
		self.stopped.set()

	def run(self):
		period = int(self.keyval.getConfig("ctrl_period_sec", default=10))
		self.keyval.setAlive(self.name, ttl=2*period)
		while not self.stopped.wait(period): # Report every five seconds
			period = int(self.keyval.getConfig("ctrl_period_sec", default=10))
			self.keyval.setAlive(self.name, ttl=2*period)


