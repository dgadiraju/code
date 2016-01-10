#!/bin/bash
# A Bash script to prepare an EC2 node for HDP installation
# v1.1
# 27JUN14 

echo "****************************"
echo "Starting Prepare Host"
echo "****************************"

#set umask
echo -e "\nSetting Umask to 022 in .bashrc"
umask 022
echo "umask 022" >> ~/.bashrc

#disable SELinux
echo -e "\nDisabling SELinux"
sudo sed -i 's/SELINUX=enforcing/SELINUX=disabled/g' /etc/selinux/config

#Turn on NTPD
echo "Setting up NTPD and syncing time"
#Need to add a check to see if NTPD is installed.  If not install it
sudo chkconfig ntpd on
sudo ntpd -q
sudo service ntpd start

# Turn off autostart of iptables and ip6tables
echo -e "\nChecking ipTables and ip6table are off"
sudo service iptables stop
sudo chkconfig iptables off
sudo service ip6tables stop
sudo chkconfig ip6tables off

#Set Swapiness
echo -e "\nSetting Swapiness to 0"
echo 0 | sudo tee /proc/sys/vm/swappiness
echo vm.swappiness = 0 | sudo tee -a /etc/sysctl.conf

#Turn on NSCD
#echo -e "\nTurning on NSCD"
#chkconfig --level 345 ncsd on
#ncsd -g

#Set File Handle Limits
echo -e "\nSetting File Handle Limits"
sudo -c 'echo hdfs – nofile 32768 >> /etc/security/limits.conf'
sudo -c 'echo mapred – nofile 32768 >> /etc/security/limits.conf'
sudo -c 'echo hbase – nofile 32768 >> /etc/security/limits.conf'
sudo -c 'echo hdfs – nproc 32768 >> /etc/security/limits.conf'
sudo -c 'echo mapred – nofile 32768 >> /etc/security/limits.conf'
sudo -c 'echo hbase – nofile 32768 >> /etc/security/limits.conf'

echo -e "\n****************************"
echo "Prepare Nodes COMPLETE!"
echo "****************************"

