#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

gunicorn -b 0.0.0.0:6786 --workers 1 \
    --worker-class aiohttp.GunicornWebWorker \
    --forwarded-allow-ips '*' \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    rssant_async.main:app
