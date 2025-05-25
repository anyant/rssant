#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas deploy-aliyun \
    --repository registry.ap-northeast-1.aliyuncs.com/rssant/rssant-asyncapi \
    --dockerfile deploy/rssant_asyncapi/Dockerfile \
    --function rssant-asyncapi \
    --build-id \
    $@
