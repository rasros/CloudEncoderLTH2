#!/bin/sh

sudo apt-get update
sudo apt-get install -y python2.7 python-setuptools python2.7-dev etcd

if [ $1 == "control" ];
then
  sudo systemctl start etcd
fi;
