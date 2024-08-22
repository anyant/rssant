#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas deploy-aliyun \
    --repository registry.cn-zhangjiakou.aliyuncs.com/rssant/rssant-async \
    --dockerfile deploy/rssant_async/Dockerfile \
    --service rssant-img1 \
    --function rssant-img1 \
    $@
