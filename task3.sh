#!/bin/bash
# This script will move a given file to a given path
# Argumant 1: Any numeric/ non-numeric value	 

echo "---- Starting Script ----"

re='^[0-9]+$'
if ! [[ $1 =~ $re ]] ; then
   echo "--- error: Not a number ---" >&2; exit 1
else
   echo "--- success: is a Number ---"
fi

echo "---- Script Ended ----"
