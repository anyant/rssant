#!/bin/bash

set -ex

rm -f /app/data/initdb.ready

# wait postgres ready
su - postgres -c 'while true; do /usr/bin/pg_isready; status=$?; if [[ $status -eq 0 ]]; then break; fi; sleep 1; done;'

# execute initdb.sql
su - postgres -c 'psql -f /app/box/initdb.sql'

# django migrate & initdb
python manage.py runscript django_pre_migrate
python manage.py migrate
python manage.py runscript django_db_init

touch /app/data/initdb.ready
exit 0
