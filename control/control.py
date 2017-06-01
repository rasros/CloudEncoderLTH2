#!/usr/bin/env python

from openstack import OpenStackVMOperations
import sys
import time

def main():
	if len(sys.argv) < 2:
		print("Please provide a prefix argument")
		sys.exit(1)
	prefix=sys.argv[1]+"-"
	IMAGE='ubuntu 16.04'

	print("Initiating OpenStack operations")
	os = OpenStackVMOperations()
	os.readConf()
	info = os.getCloudInfo()
	if len(info['zones']) <= 1:
		print("Single availability zone")
	else:
		print('Availability zones: {}'.format(', '.join(info['zones'])))

	if not IMAGE in os.listImageNames():
		print("Image {} not found".format(IMAGE))
		sys.exit(1)

	for vm in os.listVMs():
		if vm.name.startswith(prefix):
			print("Killing lingering machine: " + vm.name)
			os.terminateVM(vm.name)

	print("Start control instance")
	vm = os.createVM(prefix+"control", imageName=IMAGE)
	time.sleep(1)
	print("Killing control instance " + vm.name)
	os.terminateVM(vm.name)

if __name__ == "__main__":
	main()
