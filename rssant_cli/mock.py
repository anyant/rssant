from urllib.parse import urljoin

import click
import rssant_common.django_setup  # noqa:F401
from django.utils import timezone
from rssant_api.helper import reverse_url
from rssant_api.models import STORY_SERVICE, Feed, FeedStatus
from rssant_common.rss import get_story_of_feed_entry
from rssant_feedlib.parser import validate_story

FEED_URL = 'https://test.rss.anyant.com/feed.xml'


def _create_test_feed(url):
    feed = Feed.get_first_by_url(url)
    if not feed:
        now = timezone.now()
        feed = Feed(
            url=url, status=FeedStatus.DISCARD,
            reverse_url=reverse_url(url),
            title='蚁阅测试订阅',
            dt_updated=now, dt_checked=now, dt_synced=now)
        feed.save()
    return feed


def _create_test_story(
    feed: Feed,
    ident: str,
    title: str,
    content: str,
    summary: str = None,
    **kwargs,
):
    story_url = urljoin(feed.url, f'/story/{ident}')
    if summary is None:
        summary = content[:80]
    story_entry = dict(
        ident=ident,
        title=title,
        url=story_url,
        content=content,
        summary=summary,
        **kwargs,
    )
    story_entry = validate_story(story_entry)
    story = get_story_of_feed_entry(story_entry)
    STORY_SERVICE.bulk_save_by_feed(feed.id, [story])


@click.group()
def main():
    """
    创建和更新测试数据，订阅: https://test.rss.anyant.com/feed.xml
    """


@main.command()
@click.option('--ident', type=str, required=False, help='story unique id')
@click.option('--content', type=str, required=False, help='story content')
@click.option('--summary', type=str, required=False, help='story summary')
@click.option('--title', type=str, required=False, help='story title')
def story(ident: str, content: str, summary: str, title: str):
    """创建测试Story"""
    if not ident:
        ident = timezone.now().strftime('%Y-%m%d-%H%M%S')
    if not title:
        title = f'测试文章{ident}'
    if not content:
        content = f'Hello {ident}!'
    feed = _create_test_feed(FEED_URL)
    _create_test_story(
        feed,
        ident=ident,
        content=content,
        summary=summary,
        title=title,
    )
    click.echo(f'story created: {ident} {title}')


if __name__ == '__main__':
    main()
