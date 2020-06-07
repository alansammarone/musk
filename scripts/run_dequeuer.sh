#!/bin/bash

LOCKFILE=/tmp/dequeuer.lock
if [ -f $LOCKFILE ]; then
	echo "Dequeuer already running."
else
	touch $LOCKFILE
    ../env/bin/python dequeuer.py
    rm $LOCKFILE
fi