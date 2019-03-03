## Components

```
network:
    rssant
volumes:
    rssant_kong_postgres
    rssant_postgres
services:
    rssant_kong_postgres
    rssant_kong_service
    rssant_kong_webui
    rssant_postgres
    rssant_redis
    rssant_web
    rssant_api
    rssant_scheduler
    rssant_worker
ports:
    6789: kong gateway
    6790: kong gateway ssl
    6787: kong webui
    6785: celery webui
    6788: rssant admin
```

## Network

```
docker network create rssant
```

## Volume

```
docker volume create rssant_kong_postgres
docker volume create rssant_postgres
```


## Kong

Start Postgres

```
docker run -d \
    --name rssant_kong_postgres \
    --network=rssant \
    -v rssant_kong_postgres:/var/lib/postgresql/data \
    -e "POSTGRES_USER=kong" \
    -e "POSTGRES_PASSWORD=kong" \
    -e "POSTGRES_DB=kong" \
    postgres:9.6
```

Init Kong Database

```
docker run --rm \
    --network=rssant \
    -e "KONG_DATABASE=postgres" \
    -e "KONG_PG_HOST=rssant_kong_postgres" \
    -e "KONG_PG_PORT=5432" \
    -e "KONG_PG_DATABASE=kong" \
    -e "KONG_PG_USER=kong" \
    -e "KONG_PG_PASSWORD=kong" \
    kong:1.0 kong migrations bootstrap
```

Start Kong Service

```
docker run -d \
    --name rssant_kong_service \
    --network=rssant \
    -e "KONG_DATABASE=postgres" \
    -e "KONG_PG_HOST=rssant_kong_postgres" \
    -e "KONG_PG_PORT=5432" \
    -e "KONG_PG_DATABASE=kong" \
    -e "KONG_PG_USER=kong" \
    -e "KONG_PG_PASSWORD=kong" \
    -e "KONG_PROXY_ACCESS_LOG=/dev/stdout" \
    -e "KONG_ADMIN_ACCESS_LOG=/dev/stdout" \
    -e "KONG_PROXY_ERROR_LOG=/dev/stderr" \
    -e "KONG_ADMIN_ERROR_LOG=/dev/stderr" \
    -e "KONG_ADMIN_LISTEN=0.0.0.0:8001, 0.0.0.0:8444 ssl" \
    -p 6789:8000 \
    -p 6790:8443 \
    kong:1.0
```

Init Kong WebUI Database

```
docker run --rm \
    --network rssant \
    pantsel/konga:0.14.1 \
    -c prepare -a postgres -u postgresql://kong:kong@rssant_kong_postgres:5432/kong_webui
```

Start Kong WebUI

```
docker run -d \
    --name rssant_kong_webui \
    --network rssant \
    -p 6787:1337 \
    -e "TOKEN_SECRET=rssant_kong_webui-secret" \
    -e "DB_ADAPTER=postgres" \
    -e "DB_HOST=rssant_kong_postgres" \
    -e "DB_PORT=5432" \
    -e "DB_USER=kong" \
    -e "DB_PASSWORD=kong" \
    -e "DB_DATABASE=kong_webui" \
    -e "NODE_ENV=production" \
    pantsel/konga:0.14.1
```

## RSSAnt

Build API Docker Image

```
docker build -t rssant/api:latest .
docker save rssant/api:latest -o data/rssant-api-latest.docker
```

Build Web Docker Image

```
docker build -t rssant/web:latest .
docker save rssant/web:latest -o data/rssant-web-latest.docker
```

Start Postgres

```
docker run -d \
    --name rssant_postgres \
    --network rssant \
    -e "POSTGRES_USER=rssant" \
    -e "POSTGRES_PASSWORD=rssant" \
    -e "POSTGRES_DB=rssant" \
    -v rssant_postgres:/var/lib/postgresql/data \
    postgres:10
```

Init Database

```
docker run --rm \
    --network rssant \
    -e "RSSANT_DEBUG=1" \
    -e "RSSANT_PG_DB=rssant" \
    -e "RSSANT_PG_HOST=rssant_postgres" \
    -e "RSSANT_PG_USER=rssant" \
    -e "RSSANT_PG_PASSWORD=rssant" \
    -e "RSSANT_REDIS_URL=redis://rssant_redis:6379/0" \
    rssant/api:latest \
    bash -c 'python manage.py migrate && python scripts/django_db_init.py'
```

Start Redis

```
docker run -d \
    --name rssant_redis \
    --network rssant \
    redis:4
```

Start RSSAnt API

```
docker run -d \
    --name rssant_api \
    --network rssant \
    -e "RSSANT_DEBUG=1" \
    -e "RSSANT_PG_DB=rssant" \
    -e "RSSANT_PG_HOST=rssant_postgres" \
    -e "RSSANT_PG_USER=rssant" \
    -e "RSSANT_PG_PASSWORD=rssant" \
    -e "RSSANT_REDIS_URL=redis://rssant_redis:6379/0" \
    -p 6788:6788 \
    rssant/api:latest \
    gunicorn -b 0.0.0.0:6788 -w 2 --threads 200 rssant.wsgi
```

Start RSSAnt Worker

```
docker run -d \
    --name rssant_worker \
    --network rssant \
    -e "RSSANT_DEBUG=1" \
    -e "RSSANT_PG_DB=rssant" \
    -e "RSSANT_PG_HOST=rssant_postgres" \
    -e "RSSANT_PG_USER=rssant" \
    -e "RSSANT_PG_PASSWORD=rssant" \
    -e "RSSANT_REDIS_URL=redis://rssant_redis:6379/0" \
    rssant/api:latest \
    celery -A rssant worker -l info
```

Start RSSAnt Scheduler

```
docker run -d \
    --name rssant_scheduler \
    --network rssant \
    -e "RSSANT_DEBUG=1" \
    -e "RSSANT_PG_DB=rssant" \
    -e "RSSANT_PG_HOST=rssant_postgres" \
    -e "RSSANT_PG_USER=rssant" \
    -e "RSSANT_PG_PASSWORD=rssant" \
    -e "RSSANT_REDIS_URL=redis://rssant_redis:6379/0" \
    rssant/api:latest \
    celery -A rssant beat -l info \
    --scheduler django_celery_beat.schedulers:DatabaseScheduler
```

Start RSSAnt Flower

```
docker run -d \
    --name rssant_flower \
    --network rssant \
    -p 6785:5555 \
    rssant/api:latest \
    celery flower --port=5555 --broker=redis://rssant_redis:6379/0
```

Start RSSAnt Web

```
docker run -d \
    --name rssant_web \
    --network rssant \
    rssant/web:latest
```
