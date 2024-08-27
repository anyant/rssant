#!/bin/bash

while true; do
    python manage.py runserver 0.0.0.0:6788
    if [ $? -eq 0 ]; then
        break
    fi
    echo '* ----------------------------------------------------------------------'
    sleep 3
done
