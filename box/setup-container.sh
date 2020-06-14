#!/bin/bash

set -e

mkdir -p logs/
mkdir -p data/
chmod a+x box/bin/*

# config postgres
sed -ri "s!^#?(listen_addresses)\s*=\s*\S+.*!\1 = '*'!" /etc/postgresql/11/main/postgresql.conf
echo 'host all all all md5' >> /etc/postgresql/11/main/pg_hba.conf
mv /var/lib/postgresql/11/main /var/lib/postgresql/11/init
mkdir /var/lib/postgresql/11/main
chown postgres /var/lib/postgresql/11/main
chgrp postgres /var/lib/postgresql/11/main
chmod 700 /var/lib/postgresql/11/main

# config supervisor
cat box/supervisord.conf > /etc/supervisord.conf

# config nginx
cat box/nginx/nginx.conf > /etc/nginx/nginx.conf
