#!/bin/bash

docker run -ti \
-p 127.0.0.1:5432:5432 \
-e POSTGRES_USER=rssant \
-e POSTGRES_PASSWORD=rssant \
-v $(pwd)/data/postgres:/var/lib/postgresql/data \
postgres:10
