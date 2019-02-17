#!/bin/bash

docker run --name rssant_redis --rm -ti \
-p 127.0.0.1:6379:6379 \
-v $(pwd)/data/redis:/data \
redis:4
