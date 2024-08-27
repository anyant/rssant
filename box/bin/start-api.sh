#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

export RSSANT_ROLE=api
export RSSANT_BIND_ADDRESS=0.0.0.0:6788
/app/runserver.py
