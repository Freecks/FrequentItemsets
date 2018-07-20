#!/bin/sh

hdfs dfs -mkdir -p input
hdfs dfs -put /home/lifoadm/Documents/data/retail.dat input/
#hdfs dfs -put /home/lifoadm/Documents/data/webdocs.dat input/
hdfs dfs -rmr output

#hdfs dfs -put ../data/webdocs.dat input/webdocs.dat


##examples are for use with webdocs, arguments are input, output, support, number of results to display and number of reducers

#Webdoc / support = 30% ~ 507625 
#hadoop jar  target/leo-0.0.1-SNAPSHOT.jar  stage.leo.App input/webdocs.dat output 507625 10 1

#retail.dat / support:1000 
hadoop jar  target/leo-0.0.1-SNAPSHOT.jar  stage.leo.App input/retail.dat output 1000 8 2
#hadoop jar  target/leo-0.0.1-SNAPSHOT.jar  stage.leo.App input/retail.dat output 300 8 8


#support = 29%
#hadoop jar FrequentItemsets.jar App input/webdocs.dat output 490704 10 32

#support = 28%
#hadoop jar FrequentItemsets.jar App input/webdocs.dat output 473783 10 32

#support = 27%
#hadoop jar FrequentItemsets.jar App input/webdocs.dat output 456862 10 32

#support = 26%
#hadoop jar target/leo-0.0.1-SNAPSHOT.jar stage.leo.App input/webdocs.dat output 439941 10 2

#support = x%
#hadoop jar target/leo-0.0.1-SNAPSHOT.jar stage.leo.App input/webdocs.dat output 100000 10 4
