#!/bin/sh

hadoop fs -mkdir /user/cloudera/wikisearch-input
hadoop fs -put /media/sf_Projects/BigDataGCW/input/part-m-00001 /user/cloudera/wikisearch-input/part-m-00001
