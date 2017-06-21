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

	### Machine sets and states

	def setStarting(self, name, app, version=0, default_attempts=24):
		attempts = self.getConfig(app+'_retries', default=default_attempts)
		self.etcd.write('/control/starting/'+name, attempts)
		self.etcd.write('/control/machines/'+app+'/'+name, version)

	def updateStarting(self, name, attempts):
		self.etcd.write('/control/starting/'+name, attempts)

	def removeStarting(self, name):
		self.etcd.delete('/control/starting/'+name)

	def getStarting(self):
		ret=[]
		try:
			for m in self.etcd.read('/control/starting').leaves:
				if m.key != "/control/starting":
					ret.append(m.key.split('/')[-1])
		except Exception as e:
			pass
		return sorted(ret)

	def getMachines(self, app):
		ret=[]
		try:
			for m in self.etcd.read('/control/machines/'+app).leaves:
				if m.key != "/control/machines/"+app:
					ret.append({'name':m.key.split('/')[-1], 'version':m.value})
		except Exception as e:
			pass
		ret.sort(key=lambda x: x['name'])
		return ret

	def clearMachine(self, app, name):
		self.etcd.delete('/control/machines/'+app+'/'+name)

	### Log to the key value store
	def log(self, name, s):
		self.append("/log/", "[{:15s}] {}".format(name[:15], s))
