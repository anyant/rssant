#!/bin/bash

celery -A rssant worker -E -l info --pool eventlet --concurrency 100 -n $1
