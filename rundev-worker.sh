#!/bin/bash

export RSSANT_ROLE=worker

while true; do
    python manage.py runserver 0.0.0.0:6793
    if [ $? -eq 0 ]; then
        break
    fi
    echo '* ----------------------------------------------------------------------'
    sleep 3
done
