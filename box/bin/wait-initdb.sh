#!/bin/bash

wait_file() {
  local file="$1"; shift
  # 180 seconds as default timeout
  local wait_seconds="${1:-180}"; shift
  until test $((wait_seconds--)) -eq 0 -o -f "$file" ; do sleep 1; done
  ((++wait_seconds))
}

wait_file "/app/data/initdb.ready"
echo initdb ready!
