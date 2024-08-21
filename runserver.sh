#!/bin/bash

gunicorn -b 0.0.0.0:9000 --threads 50 \
    --forwarded-allow-ips '*' \
    --access-logfile - \
    --error-logfile - \
    --log-level info \
    rssant.wsgi
