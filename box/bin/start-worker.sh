#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

export RSSANT_ROLE=worker
gunicorn -b 0.0.0.0:6793 --threads 30 \
    --forwarded-allow-ips '*' \
    --reuse-port \
    --timeout=300 \
    --keep-alive=7200 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    rssant.wsgi
