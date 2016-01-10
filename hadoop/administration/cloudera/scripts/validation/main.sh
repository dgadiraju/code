HOST=$1
CMD=$2
for i in {2..7};
do
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "${CMD}"`
done
