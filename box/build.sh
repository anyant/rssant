#!/bin/bash

# git clone https://gitee.com/anyant/rssant-web.git box/web
# pushd box/web; git pull; popd

docker build -f box/Dockerfile -t rssant/box:latest . $@
