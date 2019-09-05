#!/bin/bash

docker network create rssant || true
docker volume create rssant-postgres || true
#!/bin/bash

docker volume create rssant_postgres

docker run -d \
    --name rssant-postgres \
    --network rssant \
    -p 127.0.0.1:5432:5432 \
    -e "POSTGRES_USER=rssant" \
    -e "POSTGRES_PASSWORD=rssant" \
    -e "POSTGRES_DB=rssant" \
    -v rssant_postgres:/var/lib/postgresql/data \
    postgres:10
