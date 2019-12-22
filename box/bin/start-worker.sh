#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

python -m rssant_worker.main \
    --concurrency 5 \
    --node rssant --port 6792 \
    --network rssant@http://localhost:6792
