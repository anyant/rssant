#!/bin/bash

set -ex

if [ ! "$(ls -A /var/lib/postgresql/11/main)" ]; then
    echo 'copy postgresql init data to mounted volume'
    cp -r /var/lib/postgresql/11/init/* /var/lib/postgresql/11/main
fi

rm -f /var/lib/postgresql/11/main/postmaster.pid
/usr/lib/postgresql/11/bin/postgres \
    -D /var/lib/postgresql/11/main \
    -c config_file=/etc/postgresql/11/main/postgresql.conf
