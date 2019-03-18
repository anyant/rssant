import click
from .finder import FeedFinder


@click.group()
def cli():
    pass


@cli.command()
@click.argument('url')
def find(url):

    def message_handler(msg):
        print(msg)

    finder = FeedFinder(url, message_handler=message_handler)
    result = finder.find()
    if result:
        print(f"Got: " + str(result.feed)[:300] + "\n")
    finder.close()


if __name__ == "__main__":
    cli()
