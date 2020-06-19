#!/bin/bash

set -e

bash ./box/run.sh
sleep 3
docker ps --latest
docker logs --tail 1000 rssant

docker run -ti guyskk/rssant:latest pytest -m 'not dbtest'

docker exec -ti rssant bash box/bin/wait-initdb.sh
docker exec -ti rssant pytest -m 'dbtest'
docker rm -f rssant || true
