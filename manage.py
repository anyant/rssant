#!/usr/bin/env python
import os
import sys
import time

from rssant_config import CONFIG
from rssant_common.logger import configure_logging


if __name__ == '__main__':
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')
    os.environ.setdefault('SERVER_WSGI', 'true')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    configure_logging(level=CONFIG.log_level)
    while True:
        try:
            execute_from_command_line(sys.argv)
        except SyntaxError as ex:
            print(f'* SyntaxError: {ex}')
            time.sleep(3)
        else:
            break
