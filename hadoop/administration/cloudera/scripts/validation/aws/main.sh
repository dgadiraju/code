HOST=$1
CMD=$2
echo "$1: " `ssh -i ~/.ec2/dgadiraju root@${1} "${CMD}"`
