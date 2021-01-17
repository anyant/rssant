#!/bin/bash
locust -f unmaintain/locustfile.py --host 'http://127.0.0.1:6788'
