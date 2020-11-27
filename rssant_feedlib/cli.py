import logging
import os

import click
import slugify
from pyinstrument import Profiler

from rssant_common.helper import pretty_format_json
from rssant_api.helper import shorten

from .finder import FeedFinder
from .raw_parser import RawFeedParser
from .parser import FeedParser
from .reader import FeedReader
from .feed_checksum import FeedChecksum
from .response_file import FeedResponseFile


LOG = logging.getLogger(__name__)


@click.group()
def cli():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)1.1s %(asctime)s %(name)s:%(lineno)-4d %(message)s"
    )


class Printer:
    def __init__(self, supress=False):
        self.supress = supress

    def __call__(self, *args, **kwargs):
        if not self.supress:
            print(*args, **kwargs)


class ProfilerContext:
    def __init__(self, profile=False):
        self.profile = profile
        self.profiler = None

    def __enter__(self):
        if self.profile:
            self.profiler = profiler = Profiler()
            profiler.start()

    def __exit__(self, *args):
        if self.profile:
            self.profiler.stop()
            print(self.profiler.output_text(unicode=True, color=True))


def _normalize_path(p):
    return os.path.abspath(os.path.expanduser(p))


def _do_find(url, max_trys, printer, rss_proxy_url, rss_proxy_token):

    def message_handler(msg):
        print(msg)

    finder = FeedFinder(
        url, max_trys=max_trys,
        rss_proxy_url=rss_proxy_url,
        rss_proxy_token=rss_proxy_token,
        message_handler=message_handler,
    )
    with finder:
        found = finder.find()
    if found:
        response, raw_result = found
        printer('-> {}'.format(response))
        result = FeedParser().parse(raw_result)
        printer("-> {}".format(result))
        printer('-' * 79)
        printer(pretty_format_json(result.feed))
        for i, story in enumerate(result.storys):
            printer('{:03d}{}'.format(i, '-' * 76))
            story['content'] = shorten(story['content'], 60)
            story['summary'] = shorten(story['summary'], 60)
            printer(pretty_format_json(story))


def _do_parse(
    url: str, printer, checksum, save_checksum,
    rss_proxy_url, rss_proxy_token,
):
    if not url.startswith('http://') and not url.startswith('https://'):
        response_file = FeedResponseFile(url)
        response = response_file.read()
    else:
        reader = FeedReader(
            rss_proxy_url=rss_proxy_url,
            rss_proxy_token=rss_proxy_token,
        )
        with reader:
            response = reader.read(url, use_proxy=reader.has_rss_proxy)
    print('-> {}'.format(response))
    if not response.ok:
        return
    if checksum:
        with open(checksum, 'rb') as f:
            data = f.read()
        checksum = FeedChecksum.load(data)
        print('-> {}'.format(checksum))
    else:
        checksum = None
    raw_result = RawFeedParser().parse(response)
    if raw_result.warnings:
        print('Warning: ' + '; '.join(raw_result.warnings))
    result = FeedParser(checksum=checksum).parse(raw_result)
    print("-> {}".format(result))
    printer('-' * 79)
    printer(pretty_format_json(result.feed))
    for i, story in enumerate(result.storys):
        printer('{:03d}{}'.format(i, '-' * 76))
        story['content'] = shorten(story['content'], 60)
        story['summary'] = shorten(story['summary'], 60)
        printer(pretty_format_json(story))
    if save_checksum:
        print('-> save {}'.format(save_checksum))
        data = result.checksum.dump()
        with open(save_checksum, 'wb') as f:
            f.write(data)


def _do_save(url, output_dir, rss_proxy_url, rss_proxy_token):
    if not output_dir:
        output_dir = os.getcwd()
    reader = FeedReader(
        rss_proxy_url=rss_proxy_url,
        rss_proxy_token=rss_proxy_token,
    )
    with reader:
        response = reader.read(url, use_proxy=reader.has_rss_proxy)
        print(f'-> {response}')
        filename = os.path.join(output_dir, slugify.slugify(url))
        response_file = FeedResponseFile(filename)
        print(f'-> save {response_file.filepath}')
        response_file.write(response)


@cli.command()
@click.argument('url')
@click.option('--max-trys', type=int, default=10, help='Max trys')
@click.option('--no-content', is_flag=True, help='Do not print feed content')
@click.option('--profile', is_flag=True, help='Run pyinstrument profile')
@click.option('--rss-proxy-url', help='rss proxy url')
@click.option('--rss-proxy-token', help='rss proxy token')
def find(
    url, max_trys, no_content=False, profile=False,
    rss_proxy_url=None, rss_proxy_token=None,
):
    printer = Printer(profile or no_content)
    with ProfilerContext(profile):
        _do_find(
            url, max_trys, printer=printer,
            rss_proxy_url=rss_proxy_url,
            rss_proxy_token=rss_proxy_token,
        )


@cli.command()
@click.argument('url')
@click.option('--output-dir', help='output dir')
@click.option('--profile', is_flag=True, help='Run pyinstrument profile')
@click.option('--rss-proxy-url', help='rss proxy url')
@click.option('--rss-proxy-token', help='rss proxy token')
def save(
    url, profile=False, output_dir=None,
    rss_proxy_url=None, rss_proxy_token=None,
):
    with ProfilerContext(profile):
        _do_save(
            url, output_dir=output_dir,
            rss_proxy_url=rss_proxy_url,
            rss_proxy_token=rss_proxy_token,
        )


@cli.command()
@click.argument('url')
@click.option('--no-content', is_flag=True, help='Do not print feed content')
@click.option('--profile', is_flag=True, help='Run pyinstrument profile')
@click.option('--rss-proxy-url', help='rss proxy url')
@click.option('--rss-proxy-token', help='rss proxy token')
@click.option('--save-checksum', help='save checksum')
@click.option('--checksum', help='feed checksum')
def parse(
    url, no_content=False, profile=False,
    rss_proxy_url=None, rss_proxy_token=None,
    save_checksum=None, checksum=None,
):
    printer = Printer(profile or no_content)
    with ProfilerContext(profile):
        _do_parse(
            url, printer=printer,
            rss_proxy_url=rss_proxy_url,
            rss_proxy_token=rss_proxy_token,
            save_checksum=save_checksum, checksum=checksum,
        )


if __name__ == "__main__":
    cli()
