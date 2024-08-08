#!/usr/bin/env bash
set -e

# This script starts as root, so we can edit /etc/hosts

# find host ip and write it into hosts file
echo host IP is `/sbin/ip route|awk '/default/ { print $3 }'`
echo writing this into /etc/hosts file

echo -e `/sbin/ip route|awk '/default/ { print $3 }'`'\t' host-machine >> /etc/hosts
echo done. Hosts file looks like this now
cat /etc/hosts


echo switching to user $CONTAINER_USER ... # probably 'afni_user'
su $CONTAINER_USER
echo running as user $USER uid $UID
# Install at runtime so that local changes will be propagated without rebuilding the image
pip3 install --prefix $PYTHONUSERBASE -e .
python3 -c "import realtimefmri"

echo "Starting realtimefmri..."
realtimefmri web_interface
