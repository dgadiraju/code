HOST=$1
for i in {2..7};
do
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "service cloudera-scm-server stop"`
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "service cloudera-scm-agent stop"`
done

for i in {2..7};
do
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "yum -y remove cloudera-manager-agent cloudera-manager-daemons cloudera-manager-server"`
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "rm -rf /etc/pki/rpm-gpg/RPM-GPG-KEY-cloudera"`
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "rm -rf /etc/security/limits.d/cloudera-scm.conf"`
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "rm -rf /var/lib/yum/repos/x86_64/6/cloudera-manager"`
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "rm -rf /var/log/cloudera-manager-installer/"`
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "rm -rf /usr/share/cmf"`
  echo "vm0${i}${HOST}:" `ssh root@vm0${i}${HOST} "rm -rf /var/cache/yum/x86_64/6/cloudera-manager"`
done

echo "run this command to validate on all hosts logging in as root: find / -name \"*cloudera*\"|grep -v stage|grep -v yum\.repos\.d"
