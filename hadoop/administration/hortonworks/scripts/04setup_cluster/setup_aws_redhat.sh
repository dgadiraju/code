#!/bin/bash
echo "Copying prepareNode.sh to nodes"
pscp -v -l ec2-user -h cluster-hosts.txt -x " -oStrictHostKeyChecking=no" prepareNode.sh /home/ec2-user/prepareNode.sh

echo "Starting prepareNode.sh on each node"
pssh -v -t 0 -l ec2-user -h cluster-hosts.txt -x "-t -t -oStrictHostKeyChecking=no" 'chmod u+x prepareNode.sh && ./prepareNode.sh >> prepareNode.log && sudo reboot' 

