HOST=$1
CMD=$2
echo "$1: " `ssh root@${1} "${CMD}"`
