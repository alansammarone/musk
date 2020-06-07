#!/bin/bash

if pgrep "dequeuer" > /dev/null
then
    ../env/bin/python enqueuer.py
else
    echo "Dequeuer already running."
fi