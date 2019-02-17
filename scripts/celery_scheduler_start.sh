#!/bin/bash

celery -A rssant beat \
-l info \
--pidfile=data/celerybeat.pid  \
--scheduler django_celery_beat.schedulers:DatabaseScheduler
