#!/bin/bash

set -ex

/usr/lib/postgresql/11/bin/postgres --version

if [ ! "$(ls -A /var/lib/postgresql/11/main)" ]; then
    echo 'copy postgresql init data to mounted volume'
    cp -a /var/lib/postgresql/11/init/. /var/lib/postgresql/11/main
fi

# the uid:gid will change after upgrade docker image
# volume permission may mismatch and need fix
# eg: when upgrade docker image from debian-9 to debian-10
chown -R postgres:postgres /var/lib/postgresql/11/main
chmod 700 /var/lib/postgresql/11/main
chown -R postgres:postgres /var/log/postgresql
chmod 700 /var/log/postgresql

rm -f /var/lib/postgresql/11/main/postmaster.pid
su -c "/usr/lib/postgresql/11/bin/postgres \
    -D /var/lib/postgresql/11/main \
    -c config_file=/etc/postgresql/11/main/postgresql.conf" \
    postgres
