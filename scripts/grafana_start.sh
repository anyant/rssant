#!/bin/bash

docker rm -f rssant-grafana
docker run -d \
    --name rssant-grafana \
    --network=rssant \
    --log-driver json-file --log-opt max-size=50m --log-opt max-file=10 \
    --restart unless-stopped \
    --memory=200M \
    --cpus=0.2 \
    -e "GF_SERVER_ROOT_URL=http://localhost:3000" \
    -e "GF_SECURITY_ADMIN_PASSWORD=grafana" \
    -p 127.0.0.1:3000:3000 \
    grafana/grafana:6.0.2
