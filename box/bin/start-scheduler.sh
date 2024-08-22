#!/bin/bash

set -ex

/app/box/bin/wait-initdb.sh

/app/run-scheduler.py
