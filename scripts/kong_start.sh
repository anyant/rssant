#!/bin/bash


docker volume create rssant_kong_postgres


## Deploy Kong Postgres Database
docker run -d --name rssant-kong-postgres \
    --network=rssant \
    --log-driver json-file --log-opt max-size=50m --log-opt max-file=10 \
    --restart unless-stopped \
    --memory=200M \
    --cpus=0.2 \
    -v rssant_kong_postgres:/var/lib/postgresql/data \
    -p 127.0.0.1:6432:5432 \
    -e "POSTGRES_DB=kong" \
    -e "POSTGRES_USER=kong" \
    -e "POSTGRES_PASSWORD=kong" \
    postgres:9.6


## Init Kong Database
docker run --rm \
    --network=rssant \
    -e "KONG_DATABASE=postgres" \
    -e "KONG_PG_HOST=rssant-kong-postgres" \
    -e "KONG_PG_PORT=5432" \
    -e "KONG_PG_DATABASE=kong" \
    -e "KONG_PG_USER=kong" \
    -e "KONG_PG_PASSWORD=kong" \
    kong:1.3.0 kong migrations bootstrap


## Start Kong
docker run -d --name rssant-kong \
    --network=rssant \
    --log-driver json-file --log-opt max-size=50m --log-opt max-file=10 \
    --restart unless-stopped \
    --memory=200M \
    --cpus=0.2 \
    -e "KONG_DATABASE=postgres" \
    -e "KONG_PG_HOST=rssant-kong-postgres" \
    -e "KONG_PG_PORT=5432" \
    -e "KONG_PG_DATABASE=kong" \
    -e "KONG_PG_USER=kong" \
    -e "KONG_PG_PASSWORD=kong" \
    -e "KONG_PROXY_ACCESS_LOG=/dev/stdout" \
    -e "KONG_ADMIN_ACCESS_LOG=/dev/stdout" \
    -e "KONG_PROXY_ERROR_LOG=/dev/stderr" \
    -e "KONG_ADMIN_ERROR_LOG=/dev/stderr" \
    -e "KONG_PROXY_LISTEN=0.0.0.0:8000" \
    -e "KONG_ADMIN_LISTEN=0.0.0.0:8001" \
    -e "KONG_MEM_CACHE_SIZE=8m" \
    -e "KONG_CLIENT_MAX_BODY_SIZE=100m" \
    -p 8000:8000 \
    -p 127.0.0.1:8001:8001 \
    kong:1.3.0
