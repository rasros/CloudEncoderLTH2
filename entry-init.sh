#!/bin/sh

# Install some packages
sudo apt-get -y update
sudo apt-get install -y python-dev
sudo apt-get install -y python-pip
cd
git clone https://github.com/rasros/CloudTranscoderLTH2.git
export LC_ALL=C

sudo echo "10.0.0.9 waspmq" >> /etc/hosts
sudo echo "129.192.68.4 xerces.ericsson.net" >> /etc/hosts


#echo "Cloning repo of the WASPY microservice"
cd CloudTranscoderLTH2
sudo python setup.py install
