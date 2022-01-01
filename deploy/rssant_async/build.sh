#!/bin/bash

set -e

IS_PUSH=$1
RSSANT_BUILD_ID="$(date -u +%Y%m%d-%H%M%S)-$(openssl rand 2 -hex)"
RSSANT_COMMIT_ID=$(git rev-parse --verify HEAD)
echo "RSSANT_BUILD_ID=$RSSANT_BUILD_ID"
echo "RSSANT_COMMIT_ID=$RSSANT_COMMIT_ID"
IMAGE_TAG=registry.cn-zhangjiakou.aliyuncs.com/rssant/async-api:"$RSSANT_BUILD_ID"

docker build \
    -f deploy/rssant_async/Dockerfile \
    -t "$IMAGE_TAG" \
    --build-arg RSSANT_BUILD_ID="$RSSANT_BUILD_ID" \
    --build-arg RSSANT_COMMIT_ID="$RSSANT_COMMIT_ID" \
    .

docker tag "$IMAGE_TAG" rssant/async-api:latest

echo "Build Success! Push by: docker push $IMAGE_TAG"
if [ "$IS_PUSH" == '--push' ]; then
    docker push "$IMAGE_TAG"
fi
