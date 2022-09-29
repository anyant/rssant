#!/bin/bash

set -e

VERSION=$1
if [ -z "$VERSION" ]; then
    VERSION='latest'
fi
echo "*** Build guyskk/rssant:$VERSION ***"

echo "*** Build --platform linux/arm64 ***"
bash box/build.sh --platform linux/arm64 -t guyskk/rssant:latest-arm64
if [ $VERSION != 'latest' ]; then
    docker tag guyskk/rssant:latest-arm64 guyskk/rssant:$VERSION-arm64
fi

echo "*** Build --platform linux/amd64 ***"
bash box/build.sh --platform linux/amd64 -t guyskk/rssant:latest-amd64
if [ $VERSION != 'latest' ]; then
    docker tag guyskk/rssant:latest-amd64 guyskk/rssant:$VERSION-amd64
fi
