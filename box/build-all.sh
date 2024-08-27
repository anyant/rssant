#!/bin/bash

set -e

VERSION=$1
if [ -z "$VERSION" ]; then
    VERSION='latest'
fi
echo "*** Build guyskk/rssant:$VERSION ***"

bash box/build.sh \
    --platform linux/amd64,linux/arm64 \
    -t "guyskk/rssant:$VERSION"
