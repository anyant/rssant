#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

/app/run-asyncapi.py --bind 0.0.0.0:6786
