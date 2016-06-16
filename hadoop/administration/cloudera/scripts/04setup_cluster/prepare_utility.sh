sudo yum -y install git
git clone http://code.google.com/p/parallel-ssh/
sudo yum -y install wget
sudo wget http://public-repo-1.hortonworks.com/ambari/centos6/2.x/updates/2.0.0/ambari.repo -P /etc/yum.repos.d
sudo yum -y install ambari-server
sudo ambari-server setup
sudo ambari-server start
sudo yum -y install telnet
sudo yum -y install postgresql-jdbc*
sudo cp /usr/share/pgsql/postgresql-*.jdbc3.jar /usr/share/java/postgresql-jdbc.jar
chmod 644 /usr/share/java/postgresql-jdbc.jar
sudo chmod 777 /tmp
sudo ambari-server setup --jdbc-db=postgres --jdbc-driver=/usr/share/java/postgresql-jdbc.jar

CREATE DATABASE hive;
CREATE USER hive WITH PASSWORD 'hive';
GRANT ALL PRIVILEGES ON DATABASE hive TO hive;

CREATE DATABASE oozie;
CREATE USER oozie WITH PASSWORD 'HDPAn@lytics';
GRANT ALL PRIVILEGES ON DATABASE oozie TO oozie;

#Setup Hue
#http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.1.3/bk_installing_manually_book/content/rpm-chap-hue-9-3-PostgreSQL.html
yum install python-devel -y 
yum install postgresql-devel -y 
yum install gcc -y
cd /usr/lib/hue 
source build/env/bin/activate 
pip install psycopg2 
/usr/lib/hue/build/env/bin/hue syncdb --noinput
