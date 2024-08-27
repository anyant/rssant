#!/bin/bash

set -ex

export PIP_CONSTRAINT="constraint.txt"
pip-compile \
    --no-emit-index-url \
    --output-file requirements.txt \
    requirements.in
