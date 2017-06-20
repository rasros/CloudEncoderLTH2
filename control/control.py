#!/usr/bin/env python

from openstack import OpenStackVMOperations
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
			self.keyval = etcd.Client(port=2379)
		else:
			self.keyval = etcd.Client(host=host, port=2379)
		self.name = myName()

	def run(self):
		period = int(readConfig(keyval, "/control/config/period_sec", default=10))
		keyval.write("/control/alive/"+self.name, 'hello', ttl=2*period)
		while not self.stopped.wait(period): # Report every five seconds
			period = int(readConfig(keyval, "/control/config/period_sec", default=10))
			keyval.write("/control/alive/"+self.name, 'hello', ttl=2*period)

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
	keyval.write("/log/", "{}: {}".format(myName(), s), append=True)

def myName():
	return os.uname()[1]

def myIp():
	global openstack
	return openstack.getVMIP(myName())

def isLeader(keyval):
	return keyval.leader['name'] == os.uname()[1]

def readConfig(keyval, path, default=None):
	try:
		res = keyval.read(path, timeout=1)
	except etcd.EtcdKeyNotFound as e:
		if not default is None:
			keyval.write(path, default)
		return default
	return res.value

def listControlVMs(openstack, prefix):
	return [vm for vm in openstack.listVMs() if vm.name.startswith(prefix+"-control-")]

def listAllVMs(openstack, prefix):
	return [vm for vm in openstack.listVMs() if vm.name.startswith(prefix+"-")]

def handleControlNodes(keyval, openstack, prefix, period):
	global IMAGE
	numnodes = int(readConfig(keyval, "/control/config/num_nodes", default=2))

	# Get all control VMs from OpenStack
	vms = listControlVMs(openstack, prefix)
	log("Control nodes:")
	for vm in vms:
		log("  {}: {}".format(vm.name, vm.status))

	# If there aren't enough VMs then boot a new one
	if len(vms) < numnodes:
		name = prefix+"-control-"+str(uuid.uuid4())
		log("Starting a new Control VM "+name)
		keyval.write('/control/starting/'+name, readConfig(keyval, "/control/config/retries", default=24))
		vm = openstack.createVM(name, imageName=IMAGE, mtype=SMALLER)
	elif len(vms) > numnodes:
		log("Num control VMs is {} > {}".format(len(vms), numnodes))
		log("TODO: Implement killing control nodes!!!")

	# List all VMs which Control node has listed as starting up
	starting = []
	try:
		res = keyval.read("/control/starting/")
		starting = [ a.key.split('/')[-1] for a in res.leaves ]
	except:
		pass

	# Set all nodes which are being configured to being alive
	for name in starting:
		keyval.write('/control/alive/'+name, 'hello', ttl=2*period)

	# Get all valid watchdogs
	alive = [ a.key.split('/')[-1] for a in keyval.read("/control/alive/").leaves ]

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
				attempts = int(keyval.read('/control/starting/'+name).value)
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
						keyval.write('/control/starting/'+name, attempts-1)
					else:
						log("Node {} not responding, moved from starting to running and will be killed soon".format(vm.name))
						keyval.delete('/control/starting/'+vm.name)
				else:
					keyval.delete('/control/starting/'+vm.name)
					log("Node {} moved from starting to running".format(vm.name))
					# Give it some time to report
					keyval.write('/control/alive/'+vm.name, 'hello', ttl=2*period)
			elif not vm.status == "BUILD":
				keyval.delete('/control/starting/'+vm.name)
				openstack.terminateVM(vm.name)
				
	
def runLeader(keyval, openstack, prefix, period):
	handleControlNodes(keyval, openstack, prefix, period)

def main():
	global IMAGE, cleankill, openstack, keyval

	if len(sys.argv) < 2:
		print("Please provide a prefix argument")
		sys.exit(1)

	prefix=sys.argv[1]

	print("Init etcd")
	stopFlag = threading.Event()
	if len(sys.argv) > 2:
		keyval = etcd.Client(host=sys.argv[2], port=2379)
		thread = NotifyThread(sys.argv[2], stopFlag)
	else:
		keyval = etcd.Client(port=2379)
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
		if cleankill:
			for vm in listAllVMs(openstack, prefix):
				log("Shutting down {}".format(vm.name))
				openstack.terminateVM(vm.name)
			break
		else:
			try:
				if int(readConfig(keyval, "/control/shutdown", default=0)) == 1:
					keyval.write("/control/shutdown", 0)
					log("*** Shutting down")
					cleankill = True

				if not cleankill:
					period = int(readConfig(keyval, "/control/config/period_sec", default=10))
					if isLeader(keyval):
						runLeader(keyval, openstack, prefix, period)
					sleepTime = -1
					while(sleepTime <= 0):
						wakeup += period
						sleepTime = wakeup-time.time()

				if int(readConfig(keyval, "/control/shutdown", default=0)) == 1:
					keyval.write("/control/shutdown", 0)
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
