#!/bin/bash

# Compile the project with hadoop on the classpath
javac -classpath ./:/home/ubuntu/Workspace/hadoop-1.1.0/hadoop-core-1.1.0.jar *.java

# Create an output directory for storing results
mkdir -p output

# Copy all the class files to the output dir
cp *.class output/

# Create a jar file from the compiled code
jar -cvf SocialNetwork.jar -C output/ .

# Run the project on the hadoop cluster
hadoop jar SocialNetwork.jar Main ./ output/ 
