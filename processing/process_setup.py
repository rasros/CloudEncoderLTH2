#!/usr/bin/env python2

from control.openstack import OpenStackVMOperations

def main():
    os = OpenStackVMOperations()
    os.readConf()
    with os.swiftConn() as swift:
        swift.put_container('test_container')

if __name__ == "__main__":
    main()
