#!/bin/bash

docker rm -f rssant-prometheus
docker run -d \
    --name rssant-prometheus \
    --network=rssant \
    --log-driver json-file --log-opt max-size=50m --log-opt max-file=10 \
    --restart unless-stopped \
    --memory=200M \
    --cpus=0.2 \
    -p 127.0.0.1:9090:9090 \
    -v $(pwd)/etc/prometheus.yml:/etc/prometheus/prometheus.yml \
    prom/prometheus:v2.12.0
