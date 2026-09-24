#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas build \
    --repository registry.ap-northeast-1.aliyuncs.com/rssant/rssant-asyncapi \
    --dockerfile deploy/rssant_asyncapi/Dockerfile \
    --build-platform linux/amd64 \
    $@
