#!/bin/bash

pyinstaller run-asyncapi.py \
    --exclude-module django \
    --exclude-module lxml \
    --exclude-module gevent \
    --hidden-import gunicorn.glogging \
    --collect-data rssant_common \
    -d noarchive \
    --noconfirm
