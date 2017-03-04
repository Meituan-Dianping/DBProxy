#!/bin/bash
basedir=$(cd `dirname $0`; pwd)/../
mkdir -p "/opt/tmp/dbproxy_log/"

####yum install 依赖包
yum install -y libevent-devel lua-devel openssl-devel flex Percona-Server-devel-55.x86_64 Percona-Server-client-55.x86_64 jemalloc jemalloc-devel glib2 glib2-devel

### install dbproxy
cd $basedir
./configure --prefix=/usr/local/mysql-proxy CFLAGS="-s -O0" && make -j 4 && make install

###mv source.cnf 
cd $basedir
mkdir -p /usr/local/mysql-proxy/conf
cp ./script/source.cnf.samples /usr/local/mysql-proxy/conf/source.cnf
