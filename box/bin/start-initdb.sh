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

# wait and init seaweedfs
python scripts/seaweedfs_wait_and_init.py http://127.0.0.1:9333/dir/assign
curl -v http://127.0.0.1:9333/dir/status?pretty=y
curl -v http://127.0.0.1:9080/status?pretty=y

touch /app/data/initdb.ready
exit 0
