#!/bin/bash

if pgrep "dequeuer" > /dev/null
then
	echo "Dequeuer already running."
else
    ../env/bin/python dequeuer.py
fi