#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas deploy-aliyun \
    --repository registry.cn-zhangjiakou.aliyuncs.com/rssant/rssant-server \
    --dockerfile deploy/rssant_server/Dockerfile \
    --function rssant-worker-qa \
    --envfile "$RSSANT_ENV_DIR/rssant-worker-qa.env" \
    --build-id \
    $@
