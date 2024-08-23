#!/bin/bash

set -e

# shellcheck disable=SC2068
ezfaas deploy-aliyun \
    --repository registry.cn-zhangjiakou.aliyuncs.com/rssant/rssant-scheduler \
    --dockerfile deploy/rssant_scheduler/Dockerfile \
    --function rssant-scheduler-qa \
    --envfile "$RSSANT_ENV_DIR/rssant-scheduler-qa.env" \
    --build-id \
    $@
