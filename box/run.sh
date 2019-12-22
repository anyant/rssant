#!/bin/bash

docker volume create rssant-data
docker volume create rssant-postgres-data
docker volume create rssant-postgres-logs

docker rm -f rssant
docker run -ti --name rssant -d \
    -p 6789:80 \
    --env-file box/rssant.env \
    -v rssant-data:/app/data \
    -v rssant-postgres-data:/var/lib/postgresql/11/main \
    -v rssant-postgres-logs:/var/log/postgresql \
    --log-driver json-file --log-opt max-size=50m --log-opt max-file=10 \
    --restart unless-stopped \
    rssant/box:latest $@

docker logs --tail 1000 -f rssant
