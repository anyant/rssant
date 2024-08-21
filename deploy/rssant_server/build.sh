#!/bin/bash

docker build \
    --platform linux/amd64 \
    -f deploy/rssant_server/Dockerfile \
    -t rssant/server \
    .
