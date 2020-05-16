#!/bin/bash

docker network create rssant || true
docker volume create rssant-postgres || true
#!/bin/bash

docker volume create rssant_postgres
docker rm -f rssant-postgres
docker run -d \
    --name rssant-postgres \
    --log-driver json-file --log-opt max-size=50m --log-opt max-file=10 \
    --restart unless-stopped \
    --memory=500M \
    --cpus=0.5 \
    --network rssant \
    -p 127.0.0.1:5432:5432 \
    -e "POSTGRES_USER=rssant" \
    -e "POSTGRES_PASSWORD=rssant" \
    -e "POSTGRES_DB=rssant" \
    -v rssant_postgres:/var/lib/postgresql/data \
    postgres:11.7
