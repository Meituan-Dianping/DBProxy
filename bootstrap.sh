#!/bin/sh 
base=$(cd "$(dirname "$0")"; pwd)
cd $base
./configure --prefix=/usr/local/mysql-proxy CFLAGS="-s -O0"
