#!/bin/sh

sudo service hadoop-hdfs-namenode restart
hdfs dfsadmin -safemode leave

sudo service hadoop-yarn-resourcemanager restart

sudo service hadoop-yarn-nodemanager restart

sudo service hadoop-hdfs-datanode restart
