#!/bin/bash
# This script will move a given file to a given path
# Argumant 1: Java class with path	 

echo "---- Starting Script ----"
mkdir out/
javac $1.java
java $1

RESULT=$?
if [ "$RESULT" -eq 0 ]
 then
    echo "--- Java Process is Running ---"
    pgrep -x "java"
    exit 0
else 
    echo "--- Java process failed. Retry ---"
    exit 1
fi

echo "---- Script Ended ----"
