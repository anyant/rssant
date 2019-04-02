#!/bin/bash

celery -A rssant worker --events -l info --pool prefork --concurrency 10 -n $1
