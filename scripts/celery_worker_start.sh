#!/bin/bash

celery -A rssant worker -l info --pool eventlet --concurrency 100 -n $1
