#!/bin/sh

##examples are for use with webdocs, arguments are input, output, support, number of results to display and number of reducers

#support = 30%
hadoop jar FrequentItemsets.jar App input/webdocs.dat output 507625 10 32

#support = 29%
#hadoop jar FrequentItemsets.jar App input/webdocs.dat output 490704 10 32

#support = 28%
#hadoop jar FrequentItemsets.jar App input/webdocs.dat output 473783 10 32

#support = 27%
#hadoop jar FrequentItemsets.jar App input/webdocs.dat output 456862 10 32

#support = 26%
#hadoop jar FrequentItemsets.jar App input/webdocs.dat output 439941 10 32
