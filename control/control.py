#!/usr/bin/env python

from openstack import OpenStackVMOperations
from keyval import KeyValueStore
from notify import NotifyThread
import os
import sys
import time
import uuid
import subprocess
import signal
import pika
import traceback

global IMAGE, openstack, keyval, VERSION
VERSIONS = {
	'ctrl': '1',
	'entry': '29',
	'worker': '7'
}
IMAGE='ubuntu 16.04'
SMALLER='c1m05'
SMALL='c1m1'
MEDIUM='c2m2'
LARGE='c2m4'

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
	keyval.log(myName(), s)

def log2(s):
	global keyval
	print(s)
	keyval.log(myName(), s)
	keyval.log(myName(), s, logpath="/ctrlog/")

def logexception(exc_type, exc_value, exc_traceback):
	log("Exception:")
	for l in traceback.format_exception(exc_type, exc_value, exc_traceback):
		log("E " + l.rstrip())

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

def handleNodeCount(keyval, openstack, prefix, period, app, nodes, machineType, nostart=False):
	''' Generic function to check the node count of a certain application
	 		and launch new VMs if necessary'''
	global IMAGE,VERSIONS
	numnodes = int(keyval.getConfig(app+"_num_nodes", default=None))

	if numnodes is None:
		return

	# If there aren't enough VMs then boot a new one
	if not nostart and len(nodes) < numnodes:
		name = prefix+str(uuid.uuid4())
		log2("Starting a new "+app+" VM "+name)
		keyval.setStarting(name, app, VERSIONS[app])
		vm = openstack.createVM(name, imageName=IMAGE, mtype=machineType)
	elif len(nodes) > numnodes:
		if app == 'worker':
			for name in nodes:
				if keyval.getWorkerFlag(name) == 0:
					log2("Shutting down worker {}".format(name))
					openstack.terminateVM(name)
					break
		else:
			log("Num "+app+" VMs is {} > {}, shutdown {}".format(len(nodes), numnodes, nodes[0]))
			openstack.terminateVM(nodes[0])

def handleStartups(keyval, openstack, prefix, period, ctrlNodes, entryNodes, workerNodes):
	''' Goes through all startup VMs and attempts to install and launch their application '''

	# List all VMs which Control node has listed as starting up
	starting = keyval.getStarting()

	# Configure the first ready VM in the list of startups and kill malfunctional
	for name in starting:
		vm = openstack.getVMDetail(name)
		if vm.status == "ACTIVE":
			attempts = int(keyval.etcd.read('/control/starting/'+name).value)
			addr = vm.networks['waspcourse']

			log2("Attempting to configure {}, attempts left: {}".format(vm.name, attempts))
			if name in ctrlNodes:
				process = subprocess.Popen(["fab", "-p", "password", "-t", "10", "-T", "60", "-D", "-i", "ctapp.pem", "-u", "ubuntu",
					'-H', addr[0], 'deploy_control:prefix={},etcdhost={}'.format(prefix, myIp())],
					stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			elif name in entryNodes:
				process = subprocess.Popen(["fab", "-p", "password", "-D", "-i", "ctapp.pem", "-u", "ubuntu",
					'-H', addr[0], 'deploy_entry:etcdhost={}'.format(myIp())],
					stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
			elif name in workerNodes:
				process = subprocess.Popen(["fab", "-p", "password", "-D", "-i", "ctapp.pem", "-u", "ubuntu",
					'-H', addr[0], 'deploy_worker:etcdhost={}'.format(myIp())],
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
				if name in entryNodes:
					ips = openstack.listFloatingIPs()
					if len(ips) > 0:
						res = vm.add_floating_ip(ips[0])
						log("Set floating ip {} {}".format(ips[0], res))
				# Give it some time to report
				keyval.setAlive(name, ttl=2*period)
				break

		elif not vm.status in ('BUILD', 'HARD_REBOOT'):
			log("{} is in bad state ({}), shutting it down".format(name, vm.status))
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

def cleanAndRestartNodes(keyval, openstack, prefix, vms, app):
	''' Creates consistency in the running VMs and machine information in the database.
			Returns the remaining listed VMs.'''
	global VERSIONS
	nodes = keyval.getMachines(app)
	for node in nodes:
		name = node['name']
		if name not in vms:
			log("Non-existing {} node {}".format(app, name))
			keyval.clearMachine(app, name)
			nodes.remove(node)
		elif node['version'] != VERSIONS[app]:
			if openstack.getVMDetail(name).status == 'ACTIVE':
				if keyval.getConfig("onversion", default='restart') == 'restart':
					log("Reboot and reinstall {} {} which is of wrong version ({} != {})".format(app,
						name, node['version'], VERSIONS[app]))
					openstack.rebootVM(name, hard=True)
					while openstack.getVMDetail(name).status == 'ACTIVE':
						time.sleep(1)
					keyval.setStarting(name, app, VERSIONS[app])
				else:
					log("Shuting down {} {} which is of wrong version ({} != {})".format(app,
						name, node['version'], VERSIONS[app]))
					openstack.terminateVM(name, hard=True)
	return nodes

def calcNbrWorker(currentNbr):
  pika_conn_params = pika.ConnectionParameters(host='waspmq', port=5672,credentials=pika.credentials.PlainCredentials('test', 'test'),)
  connection = pika.BlockingConnection(pika_conn_params)
  channel = connection.channel()
  queue = channel.queue_declare(queue="task_queue", durable=True, exclusive=False, auto_delete=False)
  num_in_queue = queue.method.message_count
  if(num_in_queue > 2 * currentNbr):
    return num_in_queue + 1
  if(num_in_queuq < currentNbr/2):
    return num_in_queue - 1

def getQueuedSize():
  pika_conn_params = pika.ConnectionParameters(host='waspmq', port=5672,credentials=pika.credentials.PlainCredentials('test', 'test'),)
  connection = pika.BlockingConnection(pika_conn_params)
  channel = connection.channel()
  queue = channel.queue_declare(queue="task_queue", durable=True, exclusive=False, auto_delete=False)
  return queue.method.message_count

def logarr(arr, indent=""):
	for n in arr:
		log2("{}{}".format(indent, n))

def runLeader(keyval, openstack, prefix, period):
	''' Main function of the control leader '''

	vms = [ vm.name for vm in listAllVMs(openstack, prefix) ]
	ctrlNodes=cleanAndRestartNodes(keyval, openstack, prefix, vms, 'ctrl')
	entryNodes=cleanAndRestartNodes(keyval, openstack, prefix, vms, 'entry')
	workerNodes=cleanAndRestartNodes(keyval, openstack, prefix, vms, 'worker')
	startingNodes=keyval.getStarting()
	ctrlNames=[ x['name'] for x in ctrlNodes ]
	entryNames=[ x['name'] for x in entryNodes ]
	workerNames=[ x['name'] for x in workerNodes ]
	allNodes = ctrlNames+entryNames+workerNames

	for vm in [ vm for vm in vms if vm not in allNodes ]:
		log("Shutting down unregistered VM " + vm)
		openstack.terminateVM(vm)

	for name in [ name for name in startingNodes if name not in vms ]:
			log("Non-existing starting node " + name)
			keyval.removeStarting(name)
			startingNodes.remove(name)

	log2("Control nodes:")
	logarr(ctrlNames, indent="  ")
	log2("Entry nodes:")
	logarr(entryNames, indent="  ")
	log2("Worker nodes:")
	logarr(workerNames, indent="  ")
	log2("Starting nodes:")
	logarr(startingNodes, indent="  ")

	try:
		numWorkers = len(workerNames)
		if numWorkers != 0:
			queued = float(getQueuedSize())
			ratio = queued/numWorkers
			log2("Job queue size: {}, ratio: {}".format(queued, ratio))
			if ratio > 1:
				keyval.putConfig("worker_num_nodes", min(8, numWorkers+1))
			elif ratio == 0:
				keyval.putConfig("worker_num_nodes", max(1, numWorkers-1))

	except Exception as e:
		exc_type, exc_value, exc_traceback = sys.exc_info()
		logexception(exc_type, exc_value, exc_traceback)

	handleNodeCount(keyval, openstack, prefix, period, 'ctrl', ctrlNames, SMALL)
	handleNodeCount(keyval, openstack, prefix, period, 'entry', entryNames, LARGE)

	nostart = False
	for worker in workerNames:
		if worker in startingNodes:
			nostart = True 
	handleNodeCount(keyval, openstack, prefix, period, 'worker', workerNames, MEDIUM, nostart=nostart)

	handleStartups(keyval, openstack, prefix, period, ctrlNames, entryNames, workerNames)
	killBadNodes(keyval, openstack, prefix, ctrlNames+entryNames+workerNames)

def checkKill(keyval):
	if int(keyval.getConfig("kill", default=0)) == 1:
		keyval.putConfig("kill", 0)
		return True
	return False

def main():
	global IMAGE, cleankill, openstack, keyval

	if len(sys.argv) < 2:
		print("Please provide a prefix argument")
		sys.exit(1)

	prefix='ct-'+sys.argv[1]+'-'

	print("Init etcd")
	if len(sys.argv) > 2:
		keyval = KeyValueStore(host=sys.argv[2], port=2379)
		notifyThread = NotifyThread(host=sys.argv[2])
	else:
		keyval = KeyValueStore(port=2379)
		notifyThread = NotifyThread()
	notifyThread.start()

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


	# Reading config once stores a default in case the parameter does not exist yet
	keyval.getConfig("ctrl_retries", default=5)
	keyval.getConfig("entry_retries", default=5)
	keyval.getConfig("ctrl_num_nodes", default=0)
	keyval.getConfig("entry_num_nodes", default=1)
	keyval.getConfig("ctrl_period_sec", default=5)
	keyval.getConfig("worker_num_nodes", default=1)
	keyval.getConfig("shutdown", default=0)
	keyval.getConfig("kill", default=0)
	keyval.getConfig("onversion", default='restart')

	wakeup = time.time()
	killed = False
	while not killed:
		log("--- Loop ---")
		if cleankill:
			for vm in listAllVMs(openstack, prefix):
				log("Shutting down {}".format(vm.name))
				openstack.terminateVM(vm.name)
			break
		else:
			try:
				if checkKill(keyval):
					killed = True
					raise Exception("*** Killed")

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

				if checkKill(keyval):
					killed = True
					raise Exception("*** Killed")

				if int(keyval.getConfig("shutdown", default=0)) == 1:
					keyval.putConfig("shutdown", 0)
					log("*** Shutting down")
					cleankill = True

				if not cleankill:
					log("Sleep {} seconds".format(sleepTime))
					time.sleep(sleepTime)

			except Exception as e:
				log(e)
	notifyThread.stop()
	log("Goodbye!")

if __name__ == "__main__":
	try:
		main()
	except Exception as e:
		log(e)

