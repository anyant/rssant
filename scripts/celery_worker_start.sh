#!/bin/bash

celery -A rssant worker -E -l info --pool prefork --concurrency 10 -n $1
