# CloudEncoderLTH2
Course work for WASP software.

# Running the application
The application uses Fabric for node deployment. You need the following

  1. If starting from outside the cloud you need the external address of a running machine and a valid private key for SSH communication.
  2. You need to create a personal OpenStack key pair called 'ctapp' and place the private key 'ctapp.pem' in the project root folder. This can be but does not have to be the same key pair used in (1).
  3. A personal OpenStack configuration in a file called 'config.properties' in the project root. There is a template file 'config.properties.template' in the git.

  fab -D -i {private key} -u ubuntu -H {main control server} deploy:prefix={personal prefix}

# etcd
The control nodes should create a etcd cluster. Currently, this cluster is not setup and etcd is run only on the initial (and only) control node. Port 2379 is monitored on all interfaces for client access. Open up port 2379 on a machine with external access to access the etcd store. For example

  etcdctl --peers http://{your server ip}:2379 member list

lists all members of the cluster.
