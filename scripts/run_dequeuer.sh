#!/bin/bash

if pgrep "dequeuer" > /dev/null
then
    ../env/bin/python dequeuer.py
else
    echo "Dequeuer already running."
fi