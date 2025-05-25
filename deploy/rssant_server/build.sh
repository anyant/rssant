#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas build \
    --repository registry.ap-northeast-1.aliyuncs.com/rssant/rssant-server \
    --dockerfile deploy/rssant_server/Dockerfile \
    --build-platform linux/amd64 \
    $@
