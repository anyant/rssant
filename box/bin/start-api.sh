#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

gunicorn -b 0.0.0.0:6788 --threads 30 \
    --forwarded-allow-ips '*' \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    rssant.wsgi
