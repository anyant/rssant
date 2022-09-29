#!/bin/bash

set -e

VERSION=$1
if [ -z "$VERSION" ]; then
    VERSION='latest'
fi

if [ $VERSION != 'latest' ]; then
    echo "*** Push guyskk/rssant:$VERSION-* ***"
    docker push guyskk/rssant:$VERSION-arm64
    docker push guyskk/rssant:$VERSION-amd64
    echo "*** Push manifest guyskk/rssant:$VERSION ***"
    docker manifest rm guyskk/rssant:$VERSION || true
    docker manifest create guyskk/rssant:$VERSION \
        --amend guyskk/rssant:$VERSION-arm64 \
        --amend guyskk/rssant:$VERSION-amd64
    docker manifest push guyskk/rssant:$VERSION
fi

echo "*** Push guyskk/rssant:latest-* ***"
docker push guyskk/rssant:latest-arm64
docker push guyskk/rssant:latest-amd64
echo "*** Push manifest guyskk/rssant:latest ***"
docker manifest rm guyskk/rssant:latest || true
docker manifest create guyskk/rssant:latest \
    --amend guyskk/rssant:latest-arm64 \
    --amend guyskk/rssant:latest-amd64
docker manifest push guyskk/rssant:latest
