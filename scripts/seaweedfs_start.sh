#!/bin/bash

docker network create rssant || true
docker volume create rssant_seaweedfs
docker rm -f rssant-seaweedfs
docker run -d \
    --name rssant-seaweedfs \
    --log-driver json-file --log-opt max-size=50m --log-opt max-file=10 \
    --restart unless-stopped \
    --memory=500M \
    --cpus=0.5 \
    --network rssant \
    -p 127.0.0.1:9333:9333 \
    -p 127.0.0.1:9080:9080 \
    -v rssant_seaweedfs:/data \
    chrislusf/seaweedfs:1.77 \
    server \
        -dir /data \
        -volume.max 2 \
        -volume.index leveldb \
        -master.port=9333 \
        -volume.port=9080 \
        -ip 127.0.0.1
