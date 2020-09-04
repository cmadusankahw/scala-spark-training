#!/bin/bash
# This script will move a given file to a given path
# Argumant 1: Source Path	 
# Argument 2: Path to move

echo "---- Starting Script ----"

mv $1/*.text $2
echo "---- File successfully moved ----"

echo "---- Script Ended ----"

