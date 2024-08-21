#!/bin/bash

docker build \
    --platform linux/amd64 \
    -f deploy/rssant_async/Dockerfile \
    -t rssant/async \
    .
