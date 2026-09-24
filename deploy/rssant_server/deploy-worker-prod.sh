#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas deploy-aliyun \
    --repository registry.ap-northeast-1.aliyuncs.com/rssant/rssant-server \
    --dockerfile deploy/rssant_server/Dockerfile \
    --function rssant-worker \
    --envfile "$RSSANT_ENV_DIR/rssant-worker-prod.env" \
    --build-id \
    $@
