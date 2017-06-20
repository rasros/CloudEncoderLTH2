from __future__ import with_statement
from fabric.api import run, local, settings, put, env

# Creates a temporary git stash from which a software archive is created
def package_software():
	with settings(warn_only=True):
		result = local('git stash | grep "^HEAD"', capture=True)
	if not result.failed:
		local('git archive "stash@{0}" -o cloudtranscoder.tar')
		local('git stash pop >/dev/null')
	else:
		local('git archive HEAD -o cloudtranscoder.tar')
	local('gzip -f cloudtranscoder.tar')

# Install common software
def install_common():
	run('sudo apt-get -q update')
	run('sudo apt-get install -q -y '+' '.join([
			"python2.7",
			"python-setuptools",
			"python2.7-dev",
			"python-pip",
			"python-cffi-backend",

			"python-keystoneauth1",
			"python-babel",
			"python-novaclient",
			"python-pika",
			"python-flask",
			"python-paramiko",

			"libssl-dev",
			"etcd",
			"openntpd",
			"gcc",
			"g++",
			"libffi-dev"
	]))

# Install control node software
def install_controller():
	run('sudo apt-get install -q -y etcd')

# Install an already packaged software
def install_application():
	put('cloudtranscoder.tar.gz', '.')
	run('tar zxf cloudtranscoder.tar.gz');
	run('sudo python setup.py install');

# Starts control node system services
def start_controller_services():
	run('sudo systemctl stop etcd')
	net=run('ifconfig', quiet=True)
	addresses = ['http://{}:2379'.format(env.host)];
	for line in net.split('\n'):
		line = line.strip()
		if line.startswith('inet addr:'):
			addresses.append('http://{}:2379'.format(line[10:].split(' ')[0]))
	local('cp etcd.env .etcd.env')
	local('echo ETCD_ADVERTISE_CLIENT_URLS=\\"{}\\" >> .etcd.env'.format(','.join(addresses)))
	put('.etcd.env', '/etc/default/etcd', use_sudo=True)
	run('sudo systemctl start etcd')

# Clears up system data for clean restart
def clear_etcd_data():
	with settings(warn_only=True):
		run('etcdctl rm --recursive /control/alive')
		run('etcdctl rm --recursive /control/starting')
		run('etcdctl rm --recursive /log/')

# Starts the controller node software
def start_controller(prefix, etcdip=None):
	# Copy openstack configuration
	put('config.properties', '.')
	# Setup SSH
	put('ctapp.pem', '.')
	# Run application
	if not etcdip is None:
		run('nohup ctcontrol {} {} &>/dev/null &'.format(prefix, etcdip), pty=False)
	else:
		run('nohup ctcontrol {} &>/dev/null &'.format(prefix), pty=False)

# Initial control node deploy from user machine
def deploy(prefix, etcdip=None):
	package_software()
	install_common()
	install_controller()
	install_application()
	start_controller_services()
	clear_etcd_data()
	start_controller(prefix, etcdip)

def deploy_control(prefix, etcdip=None):
	install_common()
	install_controller()
	install_application()
#	start_controller_services()
#	clear_etcd_data()
	start_controller(prefix, etcdip)
