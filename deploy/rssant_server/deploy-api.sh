#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas deploy-aliyun \
    --repository registry.cn-zhangjiakou.aliyuncs.com/rssant/rssant-server \
    --dockerfile deploy/rssant_server/Dockerfile \
    --function rssant-api-qa \
    --envfile "$RSSANT_ENV_DIR/rssant-api-qa.env" \
    --build-id \
    $@
