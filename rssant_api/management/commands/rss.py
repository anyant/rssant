import logging

import tqdm
from django.db import transaction, connection
import djclick as click

from rssant_api.models import Feed, Story
from rssant_common.helper import format_table
from rssant_common import unionid
from rssant_api.tasks import rss


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
@click.argument('feed-id')
def sync_feed(feed_id):
    async_result = rss.sync_feed.delay(feed_id=feed_id)
    LOG.info(f'celery task id {async_result.id}')


@main.command()
@click.argument('feed-id')
def refresh_feed_storys(feed_id):
    feed = Feed.objects.get(pk=feed_id)
    storys = list(Story.objects.filter(feed_id=feed_id).order_by('offset').all())
    LOG.info(f'refresh_feed_storys feed_id={feed_id} num_storys={len(storys)}')
    rss.fetch_feed_storys(feed, storys, is_refresh=True)


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
def clean_celery_tables():
    sql = """
    truncate
    django_celery_beat_crontabschedule,
    django_celery_beat_intervalschedule,
    django_celery_beat_solarschedule,
    django_celery_beat_periodictask,
    django_celery_beat_periodictasks,
    django_celery_results_taskresult;
    """
    LOG.info('truncate django_celery_* tables')
    with connection.cursor() as cursor:
        cursor.execute(sql)
