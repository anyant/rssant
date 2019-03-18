#!/bin/bash

celery -A rssant worker -l info --pool eventlet --concurrency 1000 -n $1
