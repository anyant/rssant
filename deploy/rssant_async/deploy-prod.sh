#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas deploy-aliyun \
    --repository registry.cn-zhangjiakou.aliyuncs.com/rssant/rssant-async \
    --dockerfile deploy/rssant_asyncapi/Dockerfile \
    --function rssant-async \
    --build-id \
    $@
