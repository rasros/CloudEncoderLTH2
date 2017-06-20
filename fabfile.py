from __future__ import with_statement
from fabric.api import run, local, settings, put, env, sudo

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
	# Kill system update tasks, we are in control
	sudo('echo 127.0.0.1 localhost $(hostname) > /etc/hosts')
	sudo('cat /etc/hosts')

	with settings(warn_only=True):
		sudo('pkill apt-get')
	sudo('dpkg --configure -a')
	sudo('apt-get -q update')
	sudo('apt-get -q -y install '+' '.join([
			"python2.7",
			"python-setuptools",
			"python-pip",
			"python-cryptography",
			"openntpd",
	]))

# Install control node software
def install_controller():
	sudo('apt-get -q -y install etcd')

# Install an already packaged software
def install_application(flavor=None):
	put('cloudtranscoder.tar.gz', '.')
	run('tar mzxf cloudtranscoder.tar.gz');
	if flavor is None:
		sudo('pip -q install -e .'.format(flavor));
	else:
		sudo('pip -q install -e .[{}]'.format(flavor));

# Starts control node system services
def start_controller_services():
	sudo('systemctl stop etcd')
	net=run('ifconfig', quiet=True)
	addresses = ['http://{}:2379'.format(env.host)];
	for line in net.split('\n'):
		line = line.strip()
		if line.startswith('inet addr:'):
			addresses.append('http://{}:2379'.format(line[10:].split(' ')[0]))
	local('cp etcd.env .etcd.env')
	local('echo ETCD_ADVERTISE_CLIENT_URLS=\\"{}\\" >> .etcd.env'.format(','.join(addresses)))
	put('.etcd.env', '/etc/default/etcd', use_sudo=True)
	sudo('systemctl start etcd')

# Clears up system data for clean restart
def clear_etcd_data():
	with settings(warn_only=True):
#		run('etcdctl rm --recursive /control/alive', quiet=True)
#		run('etcdctl rm --recursive /control/starting', quiet=True)
		run('etcdctl rm --recursive /log/', quiet=True)

# Starts the controller node software
def start_controller(prefix, etcdhost=None, foreground=None):
	# Copy openstack configuration
	put('config.properties', '.')
	# Setup SSH
	put('ctapp.pem', '.')
	# Run application
	if not etcdhost is None:
		run('nohup ctcontrol {} {} &>/dev/null &'.format(prefix, etcdhost), pty=False)
	else:
		if foreground is None:
			run('nohup ctcontrol {} &>/dev/null &'.format(prefix), pty=False)
		else:
			run('ctcontrol {}'.format(prefix))

# Start an entry node
def start_entry(etcdhost, foreground=None):
	sudo('echo 10.0.0.9 waspmq >> /etc/hosts')
	# Run application
	if foreground is None:
		run('nohup ctentry {} &>/dev/null &'.format(etcdhost), pty=False)
	else:
		run('ctentry {}'.format(etcdhost))

# Initial control node deploy from user machine
def deploy(prefix, etcdhost=None, foreground=None):
	package_software()
	install_common()
	install_controller()
	install_application()
	start_controller_services()
	clear_etcd_data()
	start_controller(prefix, etcdhost, foreground)

def deploy_control(prefix, etcdhost=None):
	install_common()
	install_controller()
	install_application()
#	start_controller_services()
#	clear_etcd_data()
	start_controller(prefix, etcdhost)

def deploy_entry(etcdhost, foreground=None):
	install_common()
	install_application()
	start_entry(etcdhost, foreground)
