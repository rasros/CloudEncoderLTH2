#!/usr/bin/env python2

from control import openstack.OpenStackVMOperations

def main():
    os = OpenStackVMOperations()
    os.readConf()
    swift = os.swiftConn()
    swift.put_container('test_container')

if __name__ == "__main__":
    main()
