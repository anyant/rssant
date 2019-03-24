from urllib.parse import unquote
import logging

import celery
from celery import shared_task as task
from django.db import transaction, connection
from django.utils import timezone

from feedlib import FeedFinder, FeedReader, FeedParser
from rssant_api.models import UserFeed, RawFeed, Feed, Story, FeedUrlMap, FeedStatus
from rssant_api.helper import shorten

LOG = logging.getLogger(__name__)


def _get_etag(response):
    return response.headers.get("ETag")


def _get_last_modified(response):
    return response.headers.get("Last-Modified")


def _get_url(response):
    return unquote(response.url)


def _get_dt_published(data):
    return data["published_parsed"] or data["updated_parsed"] or None


def _get_dt_updated(data):
    return data["updated_parsed"] or data["published_parsed"] or timezone.now()


def _get_story_unique_id(entry):
    unique_id = entry['id']
    if not unique_id:
        unique_id = entry['link']
    return unquote(unique_id)


def _update_feed_content_info(feed, raw_feed):
    feed.content_length = raw_feed.content_length
    feed.content_hash_method = raw_feed.content_hash_method
    feed.content_hash_value = raw_feed.content_hash_value
    feed.save()


def _create_raw_feed(feed, response):
    res = response
    raw_feed = RawFeed(feed=feed)
    raw_feed.url = feed.url
    raw_feed.encoding = res.encoding
    raw_feed.status_code = res.status_code
    raw_feed.etag = _get_etag(res)
    raw_feed.last_modified = _get_last_modified(res)
    headers = {}
    for k, v in res.headers.items():
        headers[k.lower()] = v
    raw_feed.headers = headers
    raw_feed.content = res.content
    raw_feed.content_length = len(res.content)
    if res.content:
        hash_method, hash_value = raw_feed.compute_content_hash(res.content)
        raw_feed.content_hash_method = hash_method
        raw_feed.content_hash_value = hash_value
    raw_feed.save()
    return raw_feed


def _save_feed(feed, parsed):
    parsed_feed = parsed.feed
    res = parsed.response
    feed.url = _get_url(res)
    feed.title = parsed_feed["title"]
    link = parsed_feed["link"]
    if not link.startswith('http'):
        # 有些link属性不是URL，用author_detail的href代替
        # 例如：'http://www.cnblogs.com/grenet/'
        author_detail = parsed_feed['author_detail']
        if author_detail:
            link = author_detail['href']
    feed.link = unquote(link)
    feed.author = parsed_feed["author"]
    feed.icon = parsed_feed["icon"] or parsed_feed["logo"]
    feed.description = parsed_feed["description"] or parsed_feed["subtitle"]
    now = timezone.now()
    feed.dt_published = _get_dt_published(parsed_feed)
    feed.dt_updated = _get_dt_updated(parsed_feed)
    feed.dt_checked = feed.dt_synced = now
    feed.etag = _get_etag(res)
    feed.last_modified = _get_last_modified(res)
    feed.encoding = res.encoding
    feed.version = parsed.version
    feed.status = FeedStatus.READY
    feed.save()
    return feed


def _create_or_update_feed(parsed):
    url = _get_url(parsed.response)
    feed = Feed.objects.filter(url=url).first()
    if feed is None:
        feed = Feed()
    return _save_feed(feed, parsed)


def _save_storys(feed, entries):
    unique_ids = [_get_story_unique_id(x) for x in entries]
    storys = {}
    q = Story.objects.filter(feed_id=feed.id, unique_id__in=unique_ids)
    for story in q.all():
        storys[story.unique_id] = story
    bulk_create_storys = []
    for data in entries:
        unique_id = _get_story_unique_id(data)
        if unique_id in storys:
            story = storys[unique_id]
        else:
            story = Story(feed=feed, unique_id=unique_id)
            storys[unique_id] = story
        content = ''
        if data["content"]:
            content = "\n<br/>\n".join([x["value"] for x in data["content"]])
        if not content:
            content = data["description"]
        if not content:
            content = data["summary"]
        summary = data["summary"]
        if not summary:
            summary = content
        summary = shorten(summary, width=300)
        now = timezone.now()
        story.content = content
        story.summary = summary
        story.title = data["title"]
        story.link = unquote(data["link"])
        story.author = data["author"]
        story.dt_published = _get_dt_published(data)
        story.dt_updated = _get_dt_updated(data)
        story.dt_synced = now
        if unique_id in storys:
            story.save()
        else:
            bulk_create_storys.append(story)
    Story.objects.bulk_create(bulk_create_storys, batch_size=100)
    return list(storys.values())


@transaction.atomic
def _save_found(user_feed, found):
    user_feed.status = FeedStatus.READY
    feed = _create_or_update_feed(found)
    user_feed_exists = UserFeed.objects.filter(user_id=user_feed.user_id, feed_id=feed.id).first()
    if user_feed_exists:
        LOG.info('UserFeed#{} user_id={} feed_id={} already exists'.format(
            user_feed_exists.id, user_feed.user_id, feed.id
        ))
        user_feed.status = FeedStatus.ERROR
        user_feed.save()
    else:
        user_feed.feed = feed
        user_feed.save()
    raw_feed = _create_raw_feed(feed, found.response)
    _update_feed_content_info(feed, raw_feed)
    _save_storys(feed, found.entries)
    url_map = FeedUrlMap(source=user_feed.url, target=feed.url)
    url_map.save()


@task(name='rssant.tasks.find_feed')
def find_feed(user_feed_id):
    messages = []

    def message_handler(msg):
        LOG.info(msg)
        messages.append(msg)

    user_feed = UserFeed.objects.get(pk=user_feed_id)
    user_feed.status = FeedStatus.UPDATING
    user_feed.save()
    start_url = user_feed.url
    finder = FeedFinder(start_url, message_handler=message_handler)
    found = finder.find()
    if not found:
        user_feed.status = FeedStatus.ERROR
        user_feed.save()
    else:
        _save_found(user_feed, found)
    return {
        'user_feed_id': user_feed_id,
        'start_url': start_url,
        'messages': messages,
    }


@task(name='rssant.tasks.check_feed')
def check_feed(seconds=300):
    now = timezone.now()
    delta = timezone.timedelta(seconds=seconds)
    dt_before = now - delta  # 正常检查时间间隔
    dt_timeout_before = now - 3 * delta  # 异常检查时间间隔
    statuses = [FeedStatus.READY, FeedStatus.ERROR]
    sql_check = """
    SELECT id FROM rssant_api_feed AS feed
    WHERE (status=ANY(%s) AND dt_checked < %s) OR (dt_checked < %s)
    ORDER BY id LIMIT 100
    """
    sql_update_status = """
    UPDATE rssant_api_feed
    SET status=%s, dt_checked=%s
    WHERE id=ANY(%s)
    """
    params = [statuses, dt_before, dt_timeout_before]
    feed_ids = []
    with connection.cursor() as cursor:
        cursor.execute(sql_check, params)
        for feed_id, in cursor.fetchall():
            feed_ids.append(feed_id)
        cursor.execute(sql_update_status, [FeedStatus.PENDING, now, feed_ids])
    LOG.info('found {} feeds need sync'.format(len(feed_ids)))
    tasks = [sync_feed.s(feed_id=feed_id) for feed_id in feed_ids]
    celery.group(tasks).apply_async()
    return dict(feed_ids=feed_ids)


def _read_response(feed):
    reader = FeedReader()
    status_code, response = reader.read(
        feed.url, etag=feed.etag, last_modified=feed.last_modified)
    return status_code, response


def _create_raw_feed_no_response(feed, status_code):
    raw_feed = RawFeed(feed=feed, status_code=status_code)
    raw_feed.save()
    return raw_feed


@task(name='rssant.tasks.sync_feed')
def sync_feed(feed_id):
    feed = Feed.objects.get(pk=feed_id)
    feed.status = FeedStatus.UPDATING
    feed.save()
    LOG.info(f'read feed#{feed_id} url={feed.url}')
    status_code, response = _read_response(feed)
    LOG.info(f'feed#{feed_id} url={feed.url} status_code={status_code}')
    if response:
        raw_feed = _create_raw_feed(feed, response)
    else:
        raw_feed = _create_raw_feed_no_response(feed, status_code)
    is_ok = 200 <= status_code <= 299
    is_ready = 200 <= status_code <= 399
    parsed = None
    if is_ok:
        if raw_feed.content_hash_value == feed.content_hash_value:
            LOG.info(f'feed#{feed_id} url={feed.url} not changed')
        else:
            LOG.info(f'parse feed#{feed_id} url={feed.url}')
            parsed = FeedParser.parse_response(response)
            if parsed.bozo:
                is_ready = False
                LOG.warning(f'failed parse feed#{feed_id} url={feed.url}: {parsed.bozo_exception}')
    num_storys = 0
    with transaction.atomic():
        if parsed and not parsed.bozo:
            _save_feed(feed, parsed)
            _update_feed_content_info(feed, raw_feed)
            num_storys = len(_save_storys(feed, parsed.entries))
        if is_ready:
            feed.status = FeedStatus.READY
        else:
            feed.status = FeedStatus.ERROR
        feed.save()
    return dict(
        feed_id=feed_id,
        url=feed.url,
        status_code=status_code,
        num_storys=num_storys,
    )


def _retry_user_feeds(user_feed_ids):
    sql_update_status = """
    UPDATE rssant_api_userfeed
    SET status=%s, dt_created=%s
    WHERE id=ANY(%s)
    """
    tasks = []
    for user_feed_id in user_feed_ids:
        tasks.append(find_feed.s(user_feed_id))
    with connection.cursor() as cursor:
        cursor.execute(sql_update_status, [FeedStatus.PENDING, timezone.now(), user_feed_ids])
    celery.group(tasks).apply_async()


@task(name='rssant.tasks.clean_user_feed')
def clean_user_feed():
    # 删除所有status=ERROR, 没有feed_id，并且入库时间超过2分钟的订阅
    q = UserFeed.objects.filter(
        status=FeedStatus.ERROR,
        feed_id__isnull=True,
        dt_created__lt=timezone.now() - timezone.timedelta(seconds=2 * 60)
    )
    num_deleted, __ = q.delete()
    LOG.info('delete {} status=ERROR and feed_id is NULL user feeds'.format(num_deleted))
    # 重试 status=UPDATING 超过10分钟的订阅
    q = UserFeed.objects.filter(
        status=FeedStatus.UPDATING,
        feed_id__isnull=True,
        dt_created__lt=timezone.now() - timezone.timedelta(seconds=10 * 60)
    )
    user_feed_ids = [x.id for x in q.only('id').all()]
    LOG.info('retry {} status=UPDATING and feed_id is NULL user feeds'.format(len(user_feed_ids)))
    _retry_user_feeds(user_feed_ids)
    # 重试 status=PENDING 超过30分钟的订阅
    q = UserFeed.objects.filter(
        status=FeedStatus.PENDING,
        feed_id__isnull=True,
        dt_created__lt=timezone.now() - timezone.timedelta(seconds=30 * 60)
    )
    user_feed_ids = [x.id for x in q.only('id').all()]
    LOG.info('retry {} status=PENDING and feed_id is NULL user feeds'.format(len(user_feed_ids)))
    _retry_user_feeds(user_feed_ids)
