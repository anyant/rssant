#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas build \
    --repository registry.cn-zhangjiakou.aliyuncs.com/rssant/rssant-asyncapi \
    --dockerfile deploy/rssant_asyncapi/Dockerfile \
    --build-platform linux/amd64 \
    $@
