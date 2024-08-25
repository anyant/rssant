#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

export RSSANT_ROLE=scheduler
export RSSANT_BIND_ADDRESS=0.0.0.0:6790
/app/runserver.py
