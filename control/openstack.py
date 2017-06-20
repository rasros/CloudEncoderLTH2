#!/usr/bin/env python2

from keystoneauth1.identity import v3
from keystoneauth1 import session
from novaclient.client import Client as NovaClient
from swiftclient import Connection as SwiftConnection
import novaclient.exceptions as NovaExceptions
import datetime
import argparse
import ConfigParser

class WaspSwiftConn:
    def readConf(self, verbose=False):
        self.verbose = verbose
        config = ConfigParser.RawConfigParser()
        config.read('config.properties')
        self.openStackUsername=config.get('user', 'username')
        self.openStackPassword=config.get('user', 'password')
        self.projectName=config.get('openstack', 'projectName')
        self.openStackAuthUrl=config.get('openstack','authUrl')
        self.openStackKeyName=config.get('openstack','keyName')
        self.openStackNetId=config.get('openstack','netId')
        self.openStackProjectDomainName=config.get('openstack','project_domain_name')
        self.openStackProjectDomainId=config.get('openstack','project_domain_id')
        self.openStackUserDomainName=config.get('openstack','user_domain_name')

    def swiftConn(self):
        _os_options = {
                'user_domain_name': self.openStackUserDomainName,
                'project_domain_name': self.openStackProjectDomainName,
                'project_name': self.projectName
        }
        return SwiftConnection(
                authurl=self.openStackAuthUrl,
                user=self.openStackUsername,
                key=self.openStackPassword,
                auth_version='3.0',
                os_options=_os_options
        )



class OpenStackVMOperations:
    def readConf(self, verbose=False):
        self.verbose = verbose
        config = ConfigParser.RawConfigParser()
        config.read('config.properties')
        self.openStackUsername=config.get('user', 'username')
        self.openStackPassword=config.get('user', 'password')
        self.projectName=config.get('openstack', 'projectName')
        self.openStackAuthUrl=config.get('openstack','authUrl')
        self.openStackKeyName=config.get('openstack','keyName')
        self.openStackNetId=config.get('openstack','netId')
        self.openStackProjectDomainName=config.get('openstack','project_domain_name')
        self.openStackProjectDomainId=config.get('openstack','project_domain_id')
        self.openStackUserDomainName=config.get('openstack','user_domain_name')

    def __init__(self, verbose=False):
        self.readConf(verbose)
        self.auth = v3.Password(
            username=self.openStackUsername,
            password=self.openStackPassword,
            project_name=self.projectName,
            auth_url= self.openStackAuthUrl,
            user_domain_name = self.openStackUserDomainName,
#            domain_name = 'xerces',
            project_domain_name = self.openStackProjectDomainName,
            project_id =self.openStackProjectDomainId
        )
        self.sess = session.Session(auth=self.auth)
        self.nova = NovaClient("2.1", session=self.sess)
        
    def out(self, *arg):
        if self.verbose:
            print(arg)

    def monitoringInfo(self,  start_date, end_date):
        usage = self.nova.usage.get( self.projectName, start_date, end_date)
        self.out(usage)

    def createFloatingIP(self, VMName):
        self.nova.floating_ip_pools.list()
        floating_ip = self.nova.floating_ips.create(self.nova.floating_ip_pools.list()[0].name)
        self.out("floating IP %s is assigned to %s VM", floating_ip.ip, name)
        instance = self.nova.servers.find_network(name=VMName)
        instance.add_floating_ip(floating_ip)

    def createVM(self, VMName, imageName="ubuntu 16.04"):
     # nova.servers.list()
        image = self.findImage(name=imageName)  # nova.images.find(name="Test") #
        flavor = self.nova.flavors.find(name="c2m2")
        net = self.nova.neutron.find_network(name=self.openStackNetId)
        nics = [{'net-id': net.id}]
        return self.nova.servers.create(name=VMName, image=image, flavor=flavor,
					key_name=self.openStackKeyName, nics=nics, userdata=open("vm-init.sh"))

    def terminateVM(self,VMName):
        instance = self.nova.servers.find(name=VMName)
        if instance == None :
            self.out("server %s does not exist" % VMName)
        else:
            self.out("deleting server..........")
            self.nova.servers.delete(instance)
            self.out("server %s deleted" % VMName)

    def listFloatingIPs(self):
        ip_list = self.nova.floating_ips.list()
        for ip in ip_list:
             self.out("fixed_ip : %s\n" % ip.fixed_ip)
             self.out("ip : %s" % ip.ip)
             self.out("instance_id : %s" % ip.instance_id)

    def listVMs(self):
        vm_list = self.nova.servers.list()
        for instance in vm_list:
            self.out("########################## #################\n")
            self.out("server id: %s\n" % instance.id)
            self.out("server name: %s\n" % instance.name)
            self.out("server image: %s\n" % instance.image)
            self.out("server flavor: %s\n" % instance.flavor)
            self.out("server key name: %s\n" % instance.key_name)
            self.out("user_id: %s\n" % instance.user_id)
            self.out("network info (mac + ip) : %s\n" % instance.networks)
            self.out("########################## #################\n\n")
        return vm_list

    def listImages(self):
      return self.nova.glance.list()

    def listImageNames(self):
      return [i.name for i in self.nova.glance.list()]

    def findImage(self, name):
      for img in self.nova.glance.list():
        if img.name == name:
          return img


    def getVMIP(self,VMName):
        instance = self.nova.servers.find(name=VMName)

        self.out("Network address info: %s\n" % instance.addresses)
        self.out("fixed ip: %s\n" % instance.networks[self.openStackNetId])
        return instance.networks[self.openStackNetId][0]


    def getVMDetail(self,VMName):
        instance = self.nova.servers.find(name=VMName)
        self.out("server id: %s\n" % instance.id)
        self.out("server name: %s\n" % instance.name)
        self.out("server image: %s\n" % instance.image)
        self.out("server flavor: %s\n" % instance.flavor)
        self.out("server key name: %s\n" % instance.key_name)
        self.out("user_id: %s\n" % instance.user_id)
        self.out("user network info: %s\n" % instance.networks)

    def getCloudInfo(self):
      ''' Get some overview information of the cloud
       A dict is returned with the following information:
        'zones': A list of availability zones
      '''
      info = {
        'zones': []
      }

      try:
        for z in self.nova.availability_zones.list(detailed=False):
          if z.zoneState['available']:
            info['zones'].append(z.zoneName)
      except NovaExceptions.Forbidden as e:
        print(e)

      return info


    def getOperation(self, args):
        if args.operation == "listIP":
            self.listFloatingIPs()
        elif args.operation == "listVM":
            self.listVMs()
        elif args.operation == "create":
            self.createVM(args.name)
        elif args.operation == "terminate":
            self.terminateVM(args.name)
        elif args.operation == "assignFIP":
            self.createFloatingIP(args.name)
        elif args.operation == "VMIP":
            self.getVMIP(args.name)
        elif args.operation == "monitor":
            self.monitoringInfo(datetime.datetime.strptime('2017-04-04 00:00:00',"%Y-%m-%d %H:%M:%S"),datetime.datetime.strptime('2017-04-05 00:00:00',"%Y-%m-%d %H:%M:%S"))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-o", "--operation",
        metavar = "VM_OPERATION",
        help = "The operation that you want to perform",
        required = True,
        choices=["create","listVM","VMIP","terminate","listIP","assignFIP","monitor"],
        dest="operation")

    parser.add_argument("-n", "--name",
        metavar = "VM_NAME",
        help = "The name  for the VM that you want to perform the operation",
        dest="name")
    args = parser.parse_args()
    ops = OpenStackVMOperations(verbose=True)
    ops.getOperation(args)
