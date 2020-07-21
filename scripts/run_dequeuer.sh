#!/bin/bash

LOCKFILE=/tmp/dequeuer.lock
GLOBALLOCKFILE=/tmp/dequeuerglobal.lock


if [ -f $GLOBALLOCKFILE ]; then
	echo "Global lock found."
else
	if [ -f $LOCKFILE ]; then
		echo "Dequeuer already running."
	else
		touch $LOCKFILE
    	../env/bin/python dequeuer_runner.py
    	rm $LOCKFILE
    fi 
fi