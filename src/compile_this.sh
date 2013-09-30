#!/bin/bash

javac -classpath ./:/home/ubuntu/Workspace/hadoop-1.1.0/hadoop-core-1.1.0.jar *.java
mkdir -p output
cp *.class output/
jar -cvf SocialNetwork.jar -C output/ .
hadoop jar SocialNetwork.jar Main ./ output/ 
