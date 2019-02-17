#!/bin/bash

celery flower --port=5555 --broker=redis://localhost:6379/0
