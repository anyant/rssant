## Components

```
network:
    rssant
volumes:
    rssant_postgres
services:
    rssant_postgres
    rssant_redis
    rssant_web
    rssant_api
    rssant_scheduler
    rssant_worker
ports:
    6785: celery webui
    6788: rssant admin
    6789: rssant web
```

## Network

```
docker network create rssant
```

## Volume

```
docker volume create rssant_postgres
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
    gunicorn -b 0.0.0.0:6788 -w 2 --threads 200 \
    --forwarded-allow-ips '*' \
    --access-logfile - \
    --error-logfile - \
    --log-level debug \
    rssant.wsgi
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
    -e "NGINX_SERVER_NAME=rssant.localhost.com" \
    -e "NGINX_UPSTREAM=rssant_api:6788" \
    -p 6789:80 \
    rssant/web:latest
```
