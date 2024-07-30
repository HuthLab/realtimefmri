#!/usr/bin/env bash
set -e

# This script starts as root, so we can edit /etc/hosts

# find host ip and write it into hosts file
echo host IP is `/sbin/ip route|awk '/default/ { print $3 }'`
echo writing this into /etc/hosts file

echo -e `/sbin/ip route|awk '/default/ { print $3 }'`'\t' host-machine >> /etc/hosts
echo done. Hosts file looks like this now
cat /etc/hosts


su $CONTAINER_USER # probably 'afni_user'
pip3 install --prefix $PYTHONUSERBASE -e .
python3 -c "import realtimefmri"

echo "Starting realtimefmri..."
realtimefmri web_interface
