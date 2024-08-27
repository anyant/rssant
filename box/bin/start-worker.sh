#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

export RSSANT_ROLE=worker
export RSSANT_BIND_ADDRESS=0.0.0.0:6793
/app/runserver.py
