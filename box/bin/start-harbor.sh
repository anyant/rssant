#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

python -m rssant_harbor.main \
    --concurrency 1 \
    --node rssant --port 6791 \
    --network rssant@http://localhost:6791
