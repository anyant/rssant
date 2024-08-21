#!/bin/bash

set -ex

export PIP_CONSTRAINT=constraint.txt
pip install -r requirements-pip.txt
pip install -r requirements.txt -r requirements-dev.txt

pre-commit install
pre-commit run --all-files
