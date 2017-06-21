#!/bin/bash

for m in $(etcdctl --peers http://ct.thefuturenow.se:2379 ls /control/machines/$1;);
do
  echo "Writing version 0 to $m";
  etcdctl --peers http://ct.thefuturenow.se:2379 set $m 0 >/dev/null;
done
