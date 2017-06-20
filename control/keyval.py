#!/usr/bin/env python2

import etcd

class KeyValueStore:
	def __init__(self, host='localhost', port=2379):
		self.etcd = etcd.Client(host=host, port=port)

	### Basic operations

	def write(self, key, value):
		self.etcd.write(key, value)

	def append(self, key, value):
		self.etcd.write(key, value, append=True)

	### Shared config

	def getConfig(self, key, default=None, timeout=1):
		path = "/config/{}".format(key)
		try:
			res = self.etcd.read(path, timeout=timeout)
		except etcd.EtcdKeyNotFound as e:
			if not default is None:
				self.etcd.write(path, default)
			return default
		return res.value

	def putConfig(self, key, value):
		path = "/config/{}".format(key)
		self.etcd.write(path, value)

	### Watchdog
	def setAlive(self, name, ttl=None):
		if ttl is None:
			period = int(self.getConfig("ctrl_period_sec", default=10))
			ttl = 2*period
		self.etcd.write("/control/alive/"+name, 1, ttl=ttl)

	def getAlive(self):
		return [ a.key.split('/')[-1] for a in self.etcd.read("/control/alive/").leaves ]
