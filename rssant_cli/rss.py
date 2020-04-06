import logging
from collections import defaultdict

import tqdm
import click
from django.utils import timezone
from django.db import transaction, connection
from django.contrib.auth import get_user_model

import rssant_common.django_setup  # noqa:F401
from rssant_api.models import Feed, Story, UnionFeed, UserStory, UserFeed
from rssant_common.helper import format_table, get_referer_of_url, pretty_format_json
from rssant_common.image_url import encode_image_url
from rssant_feedlib.reader import FeedResponseStatus, FeedReader
from rssant_common import unionid
from rssant_feedlib import processor
from rssant_common.actor_client import scheduler
from rssant_config import CONFIG


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


def _get_story_ids(option_storys):
    if option_storys:
        story_ids = option_storys.strip().split(',')
    else:
        story_ids = [story.id for story in Story.objects.only('id').all()]
    return story_ids


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
def update_feed_monthly_story_count(feeds=None):
    feed_ids = _get_feed_ids(feeds)
    LOG.info('total %s feeds', len(feed_ids))
    for feed_id in tqdm.tqdm(feed_ids, ncols=80, ascii=True):
        with transaction.atomic():
            Story.refresh_feed_monthly_story_count(feed_id)


@main.command()
@click.option('--feeds', help="feed ids, separate by ','")
def update_feed_dryness(feeds=None):
    feed_ids = _get_feed_ids(feeds)
    LOG.info('total %s feeds', len(feed_ids))
    for feed_id in tqdm.tqdm(feed_ids, ncols=80, ascii=True):
        with transaction.atomic():
            feed = Feed.get_by_pk(feed_id)
            if feed.total_storys <= 0:
                continue
            cnt = feed.monthly_story_count
            if not cnt:
                Story.refresh_feed_monthly_story_count(feed_id)
            feed.refresh_from_db()
            feed.dryness = feed.monthly_story_count.dryness()
            feed.save()


@main.command()
@click.option('--feeds', help="feed ids, separate by ','")
def update_feed_dt_first_story_published(feeds=None):
    feed_ids = _get_feed_ids(feeds)
    LOG.info('total %s feeds', len(feed_ids))
    for feed_id in tqdm.tqdm(feed_ids, ncols=80, ascii=True):
        with transaction.atomic():
            feed = Feed.get_by_pk(feed_id)
            if feed.dt_first_story_published:
                continue
            if feed.total_storys <= 0:
                continue
            try:
                story = Story.get_by_offset(feed_id, 0, detail=True)
            except Story.DoesNotExist:
                LOG.warning(f'story feed_id={feed_id} offset=0 not exists')
                continue
            feed.dt_first_story_published = story.dt_published
            feed.save()


@main.command()
@click.option('--storys', help="story ids, separate by ','")
def update_story_has_mathjax(storys=None):
    story_ids = _get_story_ids(storys)
    LOG.info('total %s storys', len(story_ids))
    for story_id in tqdm.tqdm(story_ids, ncols=80, ascii=True):
        with transaction.atomic():
            story = Story.objects.only('id', 'content', '_version').get(pk=story_id)
            if processor.story_has_mathjax(story.content):
                story.has_mathjax = True
                story.save()


@main.command()
def update_story_is_user_marked():
    user_storys = list(
        UserStory.objects
        .exclude(is_watched=False, is_favorited=False)
        .all()
    )
    LOG.info('total %s user marked storys', len(user_storys))
    if not user_storys:
        return
    for user_story in tqdm.tqdm(user_storys, ncols=80, ascii=True):
        Story.set_user_marked_by_id(user_story.story_id)


@main.command()
@click.option('--storys', help="story ids, separate by ','")
def process_story_links(storys=None):
    story_ids = _get_story_ids(storys)
    LOG.info('total %s storys', len(story_ids))
    for story_id in tqdm.tqdm(story_ids, ncols=80, ascii=True):
        with transaction.atomic():
            story = Story.objects.only('id', 'content', '_version').get(pk=story_id)
            content = processor.process_story_links(story.content, story.link)
            if story.content != content:
                story.content = content
                story.save()


@main.command()
@click.option('--storys', help="story ids, separate by ','")
def update_story_images(storys=None):
    story_ids = _get_story_ids(storys)
    LOG.info('total %s storys', len(story_ids))
    for story_id in tqdm.tqdm(story_ids, ncols=80, ascii=True):
        story = Story.objects.get(pk=story_id)
        scheduler.tell('harbor_rss.update_story_images', dict(
            story_id=story_id,
            story_url=story.link,
            images=[],
        ))


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


@main.command()
def fix_user_story_offset():
    sql = """
    SELECT us.id, us."offset", story."offset"
    FROM rssant_api_userstory AS us
    LEFT OUTER JOIN rssant_api_story AS story
        ON us.story_id=story.id
    WHERE us."offset" != story."offset"
    """
    items = []
    with connection.cursor() as cursor:
        cursor.execute(sql)
        for us_id, us_offset, story_offset in cursor.fetchall():
            items.append((us_id, us_offset, story_offset))
    click.echo(f'total {len(items)} mismatch user story offset')
    if not items:
        return
    with transaction.atomic():
        for us_id, us_offset, story_offset in tqdm.tqdm(items, ncols=80, ascii=True):
            UserStory.objects.filter(pk=us_id).update(offset=-us_offset)
        for us_id, us_offset, story_offset in tqdm.tqdm(items, ncols=80, ascii=True):
            UserStory.objects.filter(pk=us_id).update(offset=story_offset)


@main.command()
def subscribe_changelog():
    changelog_url = CONFIG.root_url.rstrip('/') + '/changelog.atom'
    feed = Feed.objects.get(url=changelog_url)
    if not feed:
        click.echo(f'not found changelog feed url={changelog_url}')
        return
    click.echo(f'changelog feed {feed}')
    User = get_user_model()
    users = list(User.objects.all())
    click.echo(f'total {len(users)} users')
    for user in tqdm.tqdm(users, ncols=80, ascii=True):
        with transaction.atomic():
            user_feed = UserFeed.objects\
                .filter(user_id=user.id, feed_id=feed.id).first()
            if not user_feed:
                user_feed = UserFeed(
                    user_id=user.id,
                    feed_id=feed.id,
                    is_from_bookmark=False,
                )
                user_feed.save()


@main.command()
def update_feed_use_proxy():
    if not CONFIG.rss_proxy_enable:
        click.echo('rss proxy not enable!')
        return
    blacklist = [
        '%博客园%',
        '%微信%',
        '%新浪%',
        '%的评论%',
        '%Comments on%',
    ]
    sql = """
    select * from rssant_api_feed
    where (NOT title LIKE ANY(%s)) AND (
        dt_created >= '2020-04-01' or
        (total_storys <= 5 and dt_updated <= '2019-12-01')
    )
    """
    feeds = list(Feed.objects.raw(sql, [blacklist]))
    click.echo(f'{len(feeds)} feeds need check')
    reader = FeedReader(
        rss_proxy_url=CONFIG.rss_proxy_url,
        rss_proxy_token=CONFIG.rss_proxy_token,
    )
    proxy_feeds = []
    with reader:
        for i, feed in enumerate(feeds):
            click.echo(f'#{i} {feed}')
            status, _ = reader.read(feed.url)
            click.echo(f'    #{i} status={FeedResponseStatus.name_of(status)}')
            if FeedResponseStatus.is_need_proxy(status):
                proxy_status, _ = reader.read(feed.url, use_proxy=True)
                click.echo(f'    #{i} proxy_status={FeedResponseStatus.name_of(proxy_status)}')
                if proxy_status == 200:
                    proxy_feeds.append(feed)
    click.echo(f'{len(proxy_feeds)} feeds need use proxy')
    if proxy_feeds:
        with transaction.atomic():
            for feed in tqdm.tqdm(proxy_feeds, ncols=80, ascii=True):
                feed.refresh_from_db()
                feed.use_proxy = True
                feed.save()


if __name__ == "__main__":
    main()
