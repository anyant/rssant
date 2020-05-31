#!/bin/bash

set -ex

/usr/bin/weed server \
    -dir /app/data/seaweedfs \
    -volume.max 1 \
    -volume.index leveldb \
    -master.port=9333 \
    -volume.port=9080 \
    -ip 127.0.0.1
