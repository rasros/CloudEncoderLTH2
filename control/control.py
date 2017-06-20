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

def listAllVMs(openstack, prefix):
	''' List all VMs using the user prefix '''
	return [vm for vm in openstack.listVMs() if vm.name.startswith(prefix)]

def handleNodeCount(keyval, openstack, prefix, period, app, nodes):
	''' Generic function to check the node count of a certain application
	 		and launch new VMs if necessary'''
	global IMAGE
	numnodes = int(keyval.getConfig(app+"_num_nodes", default=1))

	# If there aren't enough VMs then boot a new one
	if len(nodes) < numnodes:
		name = prefix+str(uuid.uuid4())
		log("Starting a new "+app+" VM "+name)
		keyval.setStarting(name, app)
		vm = openstack.createVM(name, imageName=IMAGE, mtype=SMALLER)
	elif len(nodes) > numnodes:
		log("Num "+app+" VMs is {} > {}, shutdown {}".format(len(nodes), numnodes, nodes[0]))
		openstack.terminateVM(nodes[0])

def handleStartups(keyval, openstack, prefix, period, ctrlNodes, entryNodes):
	''' Goes through all startup VMs and attempts to install and launch their application '''

	# List all VMs which Control node has listed as starting up
	starting = keyval.getStarting()

	log("Starting nodes: {}".format(starting))

	# Configure the first ready VM in the list of startups and kill malfunctional
	for name in starting:
		vm = openstack.getVMDetail(name)
		if vm.status == "ACTIVE":
			attempts = int(keyval.etcd.read('/control/starting/'+name).value)
			addr = vm.networks['waspcourse']

			log("Attempting to configure {}, attempts left: {}".format(vm.name, attempts))
			if name in ctrlNodes:
				process = subprocess.Popen(["fab", "-D", "-i", "ctapp.pem", "-u", "ubuntu",
					'-H', addr[0], 'deploy_control:prefix={},etcdip={}'.format(prefix, myIp())],
					stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			elif name in entryNodes:
				process = subprocess.Popen(["fab", "-D", "-i", "ctapp.pem", "-u", "ubuntu",
					'-H', addr[0], 'deploy_entry:prefix={},etcdip={}'.format(prefix, myIp())],
					stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

			while process.poll() is None:
				for line in iter(process.stdout.readline, b''):
					log(">>> " + line.rstrip())
			if not process.returncode == 0:
				if attempts > 0:
					keyval.updateStarting(name, attempts-1)
				else:
					log("Node {} not responding, killing it".format(name))
					keyval.removeStarting(name)
					openstack.terminateVM(name)
			else:
				keyval.removeStarting(name)
				log("Node {} moved from starting to running".format(name))
				# Give it some time to report
				keyval.setAlive(name, ttl=2*period)

		elif not vm.status == "BUILD":
			keyval.removeStarting(name)
			openstack.terminateVM(name)

	# Set all nodes which are being configured to being alive
	for name in starting:
		keyval.setAlive(name, ttl=2*period)


def killBadNodes(keyval, openstack, prefix, allNodes):
	''' Kill nodes which have not reported in '''
	# Get all valid watchdogs
	alive = keyval.getAlive()

	# Filter out VMs which have not reset their watchdog
	gone = [ name for name in allNodes if name not in alive ]

	# Shut down VMs which have not reported
	for name in gone:
		log("Shutting down {}, it has not notified in a while".format(name))
		openstack.terminateVM(name)

def runLeader(keyval, openstack, prefix, period):
	''' Main function of the control leader '''

	ctrlNodes=keyval.getMachines('ctrl')
	entryNodes=keyval.getMachines('entry')
	allNodes = ctrlNodes+entryNodes
	vms = [ vm.name for vm in listAllVMs(openstack, prefix) ]

	for vm in [ vm for vm in vms if vm not in allNodes ]:
		log("Shutting down unregistered VM " + vm)
		openstack.terminateVM(vm)

	for node in [ node for node in ctrlNodes if node not in vms ]:
		log("Non-existing control node " + node)
		keyval.clearMachine('ctrl', node)
		ctrlNodes.remove(node)

	for node in [ node for node in entryNodes if node not in vms ]:
		log("Non-existing entry node " + node)
		keyval.clearMachine('entry', node)
		entryNodes.remove(node)

	log("Control nodes: {}".format(ctrlNodes))
	log("Entry nodes: {}".format(entryNodes))

	handleNodeCount(keyval, openstack, prefix, period, 'ctrl', ctrlNodes)
	handleNodeCount(keyval, openstack, prefix, period, 'entry', entryNodes)
	handleStartups(keyval, openstack, prefix, period, ctrlNodes, entryNodes)
	killBadNodes(keyval, openstack, prefix, ctrlNodes+entryNodes)

def main():
	global IMAGE, cleankill, openstack, keyval

	if len(sys.argv) < 2:
		print("Please provide a prefix argument")
		sys.exit(1)

	prefix='ct-'+sys.argv[1]+'-'

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
