#!/usr/bin/env python

from openstack import OpenStackVMOperations
from keyval import KeyValueStore
import os
import sys
import time
import etcd
import uuid
import subprocess
import signal
import threading

global IMAGE, openstack, keyval
IMAGE='ubuntu 16.04'
SMALLER='c1m05'
SMALL='c1m1'
MEDIUM='c2m2'

class NotifyThread(threading.Thread):
	def __init__(self, host, event):
		threading.Thread.__init__(self)
		self.stopped = event
		if host is None:
			self.keyval = KeyValueStore(port=2379)
		else:
			self.keyval = KeyValueStore(host=host, port=2379)
		self.name = myName()

	def run(self):
		period = int(keyval.getConfig("ctrl_period_sec", default=10))
		keyval.setAlive(self.name, ttl=2*period)
		while not self.stopped.wait(period): # Report every five seconds
			period = int(keyval.getConfig("ctrl_period_sec", default=10))
			keyval.setAlive(self.name, ttl=2*period)

global sigint_org, cleankill
cleankill = False
def siginthandler(signum, stackframe):
	global sigint_org,cleankill
	log("*** Starting clean shutdown, interrupt again to force it")
	cleankill = True
	signal.signal(signal.SIGINT, sigint_org)
sigint_org = signal.signal(signal.SIGINT, siginthandler)

def log(s):
	global keyval
	print(s)
	keyval.append("/log/", "{}: {}".format(myName(), s))

def myName():
	return os.uname()[1]

def myIp():
	global openstack
	return openstack.getVMIP(myName())

def isLeader(keyval):
	return keyval.etcd.leader['name'] == os.uname()[1]

def listControlVMs(openstack, prefix):
	return [vm for vm in openstack.listVMs() if vm.name.startswith(prefix+"-control-")]

def listEntryVMs(openstack, prefix):
	return [vm for vm in openstack.listVMs() if vm.name.startswith(prefix+"-entry-")]

def listAllVMs(openstack, prefix):
	return [vm for vm in openstack.listVMs() if vm.name.startswith(prefix+"-")]

def handleControlNodes(keyval, openstack, prefix, period):
	global IMAGE
	numnodes = int(keyval.getConfig("ctrl_num_nodes", default=1))

	# Get all control VMs from OpenStack
	vms = listControlVMs(openstack, prefix)
	log("Control nodes:")
	for vm in vms:
		log("  {}: {}".format(vm.name, vm.status))

	# If there aren't enough VMs then boot a new one
	if len(vms) < numnodes:
		name = prefix+"-control-"+str(uuid.uuid4())
		log("Starting a new Control VM "+name)
		keyval.etcd.write('/control/starting/'+name, keyval.getConfig("ctrl_retries", default=24))
		vm = openstack.createVM(name, imageName=IMAGE, mtype=SMALLER)
	elif len(vms) > numnodes:
		log("Num control VMs is {} > {}".format(len(vms), numnodes))
		log("TODO: Implement killing control nodes!!!")

	# List all VMs which Control node has listed as starting up
	starting = []
	try:
		res = keyval.etcd.read("/control/starting/")
		starting = [ a.key.split('/')[-1] for a in res.leaves ]
	except:
		pass

	# Set all nodes which are being configured to being alive
	for name in starting:
		keyval.setAlive(name, ttl=2*period)

	# Get all valid watchdogs
	alive = keyval.getAlive()

	# Filter out VMs which have not reset their watchdog
	gone = [ i for i in range(0, len(vms)) if vms[i].name not in alive ]

	# Shut down the first VM which had not reported 
	if len(gone) != 0:
		idx = gone[0]
		log("Shutting down {} which has not reported in".format(vms[idx].name))
		openstack.terminateVM(vms[idx].name)

	# Configure the first ready VM in the list of startups and kill malfunctional
	for vm in vms:
		if vm.name in starting:
			if vm.status == "ACTIVE":
				attempts = int(keyval.etcd.read('/control/starting/'+name).value)
				addr = vm.networks['waspcourse']
				log("Attempting to configure {}, attempts left: {}".format(vm.name, attempts))
				process = subprocess.Popen(["fab", "-D", "-i", "ctapp.pem", "-u", "ubuntu",
					'-H', addr[0], 'deploy_control:prefix={},etcdip={}'.format(prefix, myIp())],
					stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
				while process.poll() is None:
					for line in iter(process.stdout.readline, b''):
						log(">>> " + line.rstrip())
				if not process.returncode == 0:
					if attempts > 0:
						keyval.etcd.write('/control/starting/'+name, attempts-1)
					else:
						log("Node {} not responding, moved from starting to running and will be killed soon".format(vm.name))
						keyval.etcd.delete('/control/starting/'+vm.name)
				else:
					keyval.etcd.delete('/control/starting/'+vm.name)
					log("Node {} moved from starting to running".format(vm.name))
					# Give it some time to report
					keyval.setAlive(vm.name, ttl=2*period)
			elif not vm.status == "BUILD":
				keyval.etcd.delete('/control/starting/'+vm.name)
				openstack.terminateVM(vm.name)

def handleEntryNodes(keyval, openstack, prefix, period):
	global IMAGE
	numnodes = int(keyval.getConfig("entry_num_nodes", default=1))

	# Get all control VMs from OpenStack
	vms = listEntryVMs(openstack, prefix)
	log("Entry nodes:")
	for vm in vms:
		log("  {}: {}".format(vm.name, vm.status))

	# If there aren't enough VMs then boot a new one
	if len(vms) < numnodes:
		name = prefix+"-entry-"+str(uuid.uuid4())
		log("Starting a new Entry VM "+name)
		keyval.etcd.write('/entry/starting/'+name, keyval.getConfig("entry_retries", default=24))
		vm = openstack.createVM(name, imageName=IMAGE, mtype=SMALLER)
	elif len(vms) > numnodes:
		log("Num entry VMs is {} > {}".format(len(vms), numnodes))
		log("TODO: Implement killing control nodes!!!")

	# List all VMs which Control node has listed as starting up
	starting = []
	try:
		res = keyval.etcd.read("/entry/starting/")
		starting = [ a.key.split('/')[-1] for a in res.leaves ]
	except:
		pass

	# Set all nodes which are being configured to being alive
	for name in starting:
		keyval.setAlive(name, ttl=2*period)

	# Get all valid watchdogs
	alive = keyval.getAlive()

	# Filter out VMs which have not reset their watchdog
	gone = [ i for i in range(0, len(vms)) if vms[i].name not in alive ]

	# Shut down the first VM which had not reported 
	if len(gone) != 0:
		idx = gone[0]
		log("Shutting down {} which has not reported in".format(vms[idx].name))
		openstack.terminateVM(vms[idx].name)

	# Configure the first ready VM in the list of startups and kill malfunctional
	for vm in vms:
		if vm.name in starting:
			if vm.status == "ACTIVE":
				attempts = int(keyval.etcd.read('/entry/starting/'+name).value)
				addr = vm.networks['waspcourse']
				log("Attempting to configure {}, attempts left: {}".format(vm.name, attempts))
				process = subprocess.Popen(["fab", "-D", "-i", "ctapp.pem", "-u", "ubuntu",
					'-H', addr[0], 'deploy_entry:prefix={},etcdip={}'.format(prefix, myIp())],
					stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
				while process.poll() is None:
					for line in iter(process.stdout.readline, b''):
						log(">>> " + line.rstrip())
				if not process.returncode == 0:
					if attempts > 0:
						keyval.etcd.write('/entry/starting/'+name, attempts-1)
					else:
						log("Node {} not responding, moved from starting to running and will be killed soon".format(vm.name))
						keyval.etcd.delete('/entry/starting/'+vm.name)
				else:
					keyval.etcd.delete('/entry/starting/'+vm.name)
					log("Node {} moved from starting to running".format(vm.name))
					# Give it some time to report
					keyval.setAlive(vm.name, ttl=2*period)
			elif not vm.status == "BUILD":
				keyval.etcd.delete('/entry/starting/'+vm.name)
				openstack.terminateVM(vm.name)
				
	
def runLeader(keyval, openstack, prefix, period):
	handleControlNodes(keyval, openstack, prefix, period)
	handleEntryNodes(keyval, openstack, prefix, period)

def main():
	global IMAGE, cleankill, openstack, keyval

	if len(sys.argv) < 2:
		print("Please provide a prefix argument")
		sys.exit(1)

	prefix=sys.argv[1]

	print("Init etcd")
	stopFlag = threading.Event()
	if len(sys.argv) > 2:
		keyval = KeyValueStore(host=sys.argv[2], port=2379)
		thread = NotifyThread(sys.argv[2], stopFlag)
	else:
		keyval = KeyValueStore(port=2379)
		thread = NotifyThread(None, stopFlag)
	thread.start()

	log("Initiating OpenStack operations")

	openstack = OpenStackVMOperations()
	openstack.readConf()
	info = openstack.getCloudInfo()
	if len(info['zones']) <= 1:
		log("Single availability zone")
	else:
		log('Availability zones: {}'.format(', '.join(info['zones'])))

	if not IMAGE in openstack.listImageNames():
		log("Image {} not found".format(IMAGE))
		sys.exit(1)

	if len(sys.argv) < 2:
		for vm in openstack.listVMs():
			if vm.name.startswith(prefix):
				log("Killing lingering machine: " + vm.name)
				openstack.terminateVM(vm.name)

	wakeup = time.time()
	while True:
		log("--- Loop ---")
		if cleankill:
			for vm in listAllVMs(openstack, prefix):
				log("Shutting down {}".format(vm.name))
				openstack.terminateVM(vm.name)
			break
		else:
			try:
				if int(keyval.getConfig("shutdown", default=0)) == 1:
					keyval.putConfig("shutdown", 0)
					log("*** Shutting down")
					cleankill = True

				if not cleankill:
					period = int(keyval.getConfig("ctrl_period_sec", default=10))
					if isLeader(keyval):
						runLeader(keyval, openstack, prefix, period)
					sleepTime = -1
					while(sleepTime <= 0):
						wakeup += period
						sleepTime = wakeup-time.time()

				if int(keyval.getConfig("shutdown", default=0)) == 1:
					keyval.putConfig("shutdown", 0)
					log("*** Shutting down")
					cleankill = True
				if not cleankill:
					log("Sleep {} seconds".format(sleepTime))
					time.sleep(sleepTime)

			except Exception as e:
				log(e)
	stopFlag.set()
	log("Goodbye!")

if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		log(e)
