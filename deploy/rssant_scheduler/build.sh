#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas build \
    --repository registry.cn-zhangjiakou.aliyuncs.com/rssant/rssant-scheduler \
    --dockerfile deploy/rssant_scheduler/Dockerfile \
    --build-platform linux/amd64 \
    $@
