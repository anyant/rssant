#!/bin/bash

docker build \
    --platform linux/amd64 \
    -f deploy/rssant_scheduler/Dockerfile \
    -t rssant/scheduler \
    .
