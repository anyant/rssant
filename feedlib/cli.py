import json
import logging

import click
from django.core.serializers.json import DjangoJSONEncoder
from pyinstrument import Profiler

from .finder import FeedFinder


def json_pretty(data):
    return json.dumps(data, cls=DjangoJSONEncoder, indent=4, ensure_ascii=False)


@click.group()
def cli():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)1.1s %(asctime)s %(name)s:%(lineno)-4d %(message)s"
    )


def _do_find(url, max_trys, raw, no_content):

    def message_handler(msg):
        print(msg)

    def output(msg):
        if not no_content:
            print(msg)

    finder = FeedFinder(url, max_trys=max_trys, message_handler=message_handler, validate=not raw)
    with finder:
        result = finder.find()
    if result:
        output(f"Got: " + str(result.feed)[:300] + "\n")
        output('-' * 79)
        output(json_pretty(result.feed))
        for i, story in enumerate(result.entries):
            output('{:03d}{}'.format(i, '-' * 76))
            output(json_pretty(story))


@cli.command()
@click.argument('url')
@click.option('--max-trys', type=int, default=10, help='Max trys')
@click.option('--raw', is_flag=True, help='Return raw feed, not validate')
@click.option('--no-content', is_flag=True, help='Do not print feed content')
@click.option('--profile', is_flag=True, help='Run pyinstrument profile')
def find(url, max_trys, raw=False, no_content=False, profile=False):
    if profile:
        no_content = True
        profiler = Profiler()
        profiler.start()
    try:
        _do_find(url, max_trys, raw, no_content)
    finally:
        if profile:
            profiler.stop()
            print(profiler.output_text(unicode=True, color=True))


if __name__ == "__main__":
    cli()
