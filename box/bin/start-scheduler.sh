#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

python -m rssant_scheduler.main \
    --concurrency 1 \
    --node rssant --port 6790 \
    --network rssant@http://localhost:6790
