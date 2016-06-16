#!/bin/bash
echo "Copying prepareNode.sh to nodes"
pscp -v -l root -h hosts.txt -x " -oStrictHostKeyChecking=no" prepareNode.sh /root/prepareNode.sh

echo "Starting prepareNode.sh on each node"
pssh -v -t 0 -l root -h hosts.txt -x "-t -t -oStrictHostKeyChecking=no" 'chmod u+x prepareNode.sh && ./prepareNode.sh >> prepareNode.log && sudo reboot' 

