#!/bin/bash
# This script will move a given file to a given path
# Argumant 1: Any String	 

echo "---- Starting Script ----"

echo " No of Characters: "
awk -F'|' '{print (NF ? NF-1 : 0)}' $1

echo "---- Script Ended ----"
