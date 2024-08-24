#!/bin/bash

set -ex

pip install -r requirements-pip.txt
PIP_CONSTRAINT=constraint.txt pip install \
    -r requirements.txt \
    -r requirements-dev.txt \
    -r requirements-build.txt

pre-commit install
pre-commit run --all-files
