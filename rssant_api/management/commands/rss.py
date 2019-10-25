import logging
from collections import defaultdict

import tqdm
from django.utils import timezone
from django.db import transaction, connection
import djclick as click

from rssant_api.models import Feed, Story, UnionFeed
from rssant_common.helper import format_table, get_referer_of_url, pretty_format_json
from rssant_common.image_url import encode_image_url
from rssant_feedlib.reader import FeedResponseStatus
from rssant_common import unionid


LOG = logging.getLogger(__name__)


@click.group()
def main():
    """RSS Commands"""


def _get_feed_ids(option_feeds):
    if option_feeds:
        feed_ids = option_feeds.strip().split(',')
    else:
        feed_ids = [feed.id for feed in Feed.objects.only('id').all()]
    return feed_ids


@main.command()
@click.option('--feeds', help="feed ids, separate by ','")
def fix_story_offset(feeds=None):
    with transaction.atomic():
        feed_ids = _get_feed_ids(feeds)
        LOG.info('total %s feeds', len(feed_ids))
        num_fixed = 0
        for feed_id in tqdm.tqdm(feed_ids, ncols=80, ascii=True):
            num_reallocate = Story.reallocate_offset(feed_id)
            if num_reallocate > 0:
                num_fixed += 1
        LOG.info('correct %s feeds', num_fixed)


@main.command()
@click.option('--dry-run', is_flag=True)
def fix_feed_total_storys(dry_run=False):
    incorrect_feeds = Story.query_feed_incorrect_total_storys()
    LOG.info('total %s incorrect feeds', len(incorrect_feeds))
    header = ['feed_id', 'total_storys', 'correct_total_storys']
    click.echo(format_table(incorrect_feeds, header=header))
    if dry_run:
        return
    with transaction.atomic():
        num_corrected = 0
        for feed_id, *__ in tqdm.tqdm(incorrect_feeds, ncols=80, ascii=True):
            fixed = Story.fix_feed_total_storys(feed_id)
            if fixed:
                num_corrected += 1
        LOG.info('correct %s feeds', num_corrected)


@main.command()
@click.option('--feeds', help="feed ids, separate by ','")
def update_feed_story_publish_period(feeds=None):
    with transaction.atomic():
        feed_ids = _get_feed_ids(feeds)
        LOG.info('total %s feeds', len(feed_ids))
        for feed_id in tqdm.tqdm(feed_ids, ncols=80, ascii=True):
            Story.update_feed_story_publish_period(feed_id)


@main.command()
@click.option('--feeds', help="feed ids, separate by ','")
def update_feed_monthly_story_count(feeds=None):
    with transaction.atomic():
        feed_ids = _get_feed_ids(feeds)
        LOG.info('total %s feeds', len(feed_ids))
        for feed_id in tqdm.tqdm(feed_ids, ncols=80, ascii=True):
            Story.refresh_feed_monthly_story_count(feed_id)


@main.command()
@click.argument('unionid_text')
def decode_unionid(unionid_text):
    numbers = unionid.decode(unionid_text)
    if len(numbers) == 3:
        click.echo('user_id={} feed_id={} offset={}'.format(*numbers))
    elif len(numbers) == 2:
        click.echo('user_id={} feed_id={}'.format(*numbers))
    else:
        click.echo(numbers)


@main.command()
@click.argument('url')
def proxy_image(url):
    referer = get_referer_of_url(url)
    token = encode_image_url(url, referer)
    click.echo(token)


@main.command()
@click.option('--days', type=int, default=1)
@click.option('--limit', type=int, default=100)
@click.option('--threshold', type=int, default=99)
def delete_invalid_feeds(days=1, limit=100, threshold=99):
    sql = """
    SELECT feed_id, title, link, url, status_code, count FROM (
        SELECT feed_id, status_code, count(1) as count FROM rssant_api_rawfeed
        WHERE dt_created >= %s and (status_code < 200 or status_code >= 400)
        group by feed_id, status_code
        having count(1) > 3
        order by count desc
        limit %s
    ) error_feed
    join rssant_api_feed
        on error_feed.feed_id = rssant_api_feed.id
    order by feed_id, status_code, count;
    """
    sql_ok_count = """
    SELECT feed_id, count(1) as count FROM rssant_api_rawfeed
    WHERE dt_created >= %s and (status_code >= 200 and status_code < 400)
        AND feed_id=ANY(%s)
    group by feed_id
    """
    t_begin = timezone.now() - timezone.timedelta(days=days)
    error_feeds = defaultdict(dict)
    with connection.cursor() as cursor:
        cursor.execute(sql, [t_begin, limit])
        for feed_id, title, link, url, status_code, count in cursor.fetchall():
            error_feeds[feed_id].update(feed_id=feed_id, title=title, link=link, url=url)
            error = error_feeds[feed_id].setdefault('error', {})
            error_name = FeedResponseStatus.name_of(status_code)
            error[error_name] = count
            error_feeds[feed_id]['error_count'] = sum(error.values())
            error_feeds[feed_id].update(ok_count=0, error_percent=100)
        cursor.execute(sql_ok_count, [t_begin, list(error_feeds)])
        for feed_id, ok_count in cursor.fetchall():
            feed = error_feeds[feed_id]
            total = feed['error_count'] + ok_count
            error_percent = round((feed['error_count'] / total) * 100)
            feed.update(ok_count=ok_count, error_percent=error_percent)
    error_feeds = list(sorted(error_feeds.values(), key=lambda x: x['error_percent'], reverse=True))
    delete_feed_ids = []
    for feed in error_feeds:
        if feed['error_percent'] >= threshold:
            delete_feed_ids.append(feed['feed_id'])
            click.echo(pretty_format_json(feed))
    if delete_feed_ids:
        confirm_delete = click.confirm(f'Delete {len(delete_feed_ids)} feeds?')
        if not confirm_delete:
            click.echo('Abort!')
        else:
            UnionFeed.bulk_delete(delete_feed_ids)
            click.echo('Done!')
    return error_feeds
