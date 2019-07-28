#!/bin/bash

docker network create rssant || true

docker run -d \
    --name rssant-redis \
    --network rssant \
    -p 127.0.0.1:6379:6379 \
    redis:4
