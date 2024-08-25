#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

export RSSANT_ROLE=asyncapi
export RSSANT_BIND_ADDRESS=0.0.0.0:6786
/app/runserver.py
