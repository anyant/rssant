#!/bin/bash

bind_address=${RSSANT_BIND_ADDRESS:-0.0.0.0:9000}
gunicorn -b "$bind_address" --threads 50 \
    --forwarded-allow-ips '*' \
    --reuse-port \
    --timeout=300 \
    --keep-alive=7200 \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    rssant.wsgi
