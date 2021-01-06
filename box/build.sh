#!/bin/bash

set -e

current_repo=$(git config --get remote.origin.url)
web_repo=$(python -c '
import sys;
p=sys.argv[1].rsplit("/",1);
w=p[1].replace("rssant","rssant-web");
print(p[0]+"/"+w)
' "$current_repo")
echo "rssant-web: $web_repo"

if [ ! -d "box/web" ]; then
    git clone "$web_repo" box/web
fi
pushd box/web || exit 1
git fetch
git checkout master
git pull --ff-only
popd || exit 1

# shellcheck disable=SC2068
docker build -f box/Dockerfile -t guyskk/rssant:latest . $@
