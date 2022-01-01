#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

python -m rssant_async.main --bind 0.0.0.0:6786
