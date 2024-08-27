#!/usr/bin/env python
import os


def scheduler_main():
    from rssant_scheduler.main import main

    main()


def asyncapi_main():
    from rssant_asyncapi.main import main

    main()


def gunicorn_main():
    bind_address = os.getenv('RSSANT_BIND_ADDRESS') or '0.0.0.0:9000'
    num_workers = int(os.getenv('RSSANT_NUM_WORKERS') or 1)
    num_threads = int(os.getenv('RSSANT_NUM_THREADS') or 50)
    gunicorn_argv = [
        'gunicorn',
        '-b',
        bind_address,
        f'--workers={num_workers}',
        f'--threads={num_threads}',
        '--forwarded-allow-ips=*',
        '--reuse-port',
        '--timeout=300',
        '--keep-alive=7200',
        '--access-logfile=-',
        '--error-logfile=-',
        '--log-level=info',
        'rssant.wsgi',
    ]
    os.execvp('gunicorn', gunicorn_argv)


def main():
    role = os.getenv('RSSANT_ROLE')
    if role == 'scheduler':
        scheduler_main()
    elif role == 'asyncapi':
        asyncapi_main()
    else:
        gunicorn_main()


if __name__ == '__main__':
    main()
