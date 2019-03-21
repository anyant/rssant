import json
import click
from .finder import FeedFinder


@click.group()
def cli():
    pass


@cli.command()
@click.argument('url')
@click.option('--raw', is_flag=True, help='Return raw feed, not validate')
def find(url, raw=False):

    def message_handler(msg):
        print(msg)

    finder = FeedFinder(url, message_handler=message_handler, validate=not raw)
    result = finder.find()
    if result:
        print(f"Got: " + str(result.feed)[:300] + "\n")
        print('-' * 79)
        print(json.dumps(result.feed, indent=4, ensure_ascii=False))
        for i, story in enumerate(result.entries):
            print('{:03d}{}'.format(i, '-' * 76))
            print(json.dumps(story, indent=4, ensure_ascii=False))
    finder.close()


if __name__ == "__main__":
    cli()
