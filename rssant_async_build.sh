#!/bin/bash

pyinstaller rssant_async_main.py \
    --exclude-module django \
    --exclude-module lxml \
    --exclude-module gevent \
    --hidden-import gunicorn.glogging \
    --collect-data rssant_common \
    -d noarchive \
    --noconfirm
