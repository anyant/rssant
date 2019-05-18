from urllib.parse import unquote
import logging

import celery
from celery import shared_task as task
from django.db import transaction
from django.utils import timezone

from rssant_feedlib import FeedFinder, FeedReader, FeedParser
from rssant.helper.content_hash import compute_hash_base64
from rssant_api.models import UserFeed, RawFeed, Feed, Story, FeedUrlMap, FeedStatus
from rssant_api.helper import shorten


LOG = logging.getLogger(__name__)


@task(name='rssant.tasks.find_feed')
def find_feed(user_feed_id):
    messages = []

    def message_handler(msg):
        LOG.info(msg)
        messages.append(msg)

    user_feed = UserFeed.get_by_pk(user_feed_id)
    user_feed.status = FeedStatus.UPDATING
    user_feed.save()
    start_url = user_feed.url
    finder = FeedFinder(start_url, message_handler=message_handler)
    found = finder.find()
    if not found:
        if user_feed.is_from_bookmark:
            user_feed.delete()
        else:
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
    feed_ids = Feed.take_outdated(seconds)
    LOG.info('found {} feeds need sync'.format(len(feed_ids)))
    tasks = [sync_feed.s(feed_id=feed_id) for feed_id in feed_ids]
    celery.group(tasks).apply_async()
    return dict(feed_ids=feed_ids)


@task(name='rssant.tasks.sync_feed')
def sync_feed(feed_id):
    feed = Feed.objects.get(pk=feed_id)
    LOG.info(f'read feed#{feed_id} url={feed.url}')
    status_code, response = _read_response(feed)
    LOG.info(f'feed#{feed_id} url={feed.url} status_code={status_code}')
    feed.refresh_from_db()
    feed.status = FeedStatus.READY
    feed.save()
    default_result = dict(
        feed_id=feed_id,
        url=feed.url,
        status_code=status_code,
        num_storys=0,
    )
    if status_code != 200 or not response:
        if status_code != 304:
            _create_raw_feed(feed, status_code, response)
        return default_result
    content_hash_base64 = compute_hash_base64(response.content)
    if not feed.is_modified(content_hash_base64):
        LOG.info(f'feed#{feed_id} url={feed.url} not modified by compare content hash!')
        return default_result
    LOG.info(f'parse feed#{feed_id} url={feed.url}')
    parsed = FeedParser.parse_response(response)
    if parsed.bozo:
        LOG.warning(f'failed parse feed#{feed_id} url={feed.url}: {parsed.bozo_exception}')
        return default_result
    num_modified, num_storys = _save_storys(feed, parsed.entries)
    feed.refresh_from_db()
    _save_feed(feed, parsed, content_hash_base64, has_update=num_modified > 0)
    if num_modified > 0:
        _create_raw_feed(feed, status_code, response, content_hash_base64=content_hash_base64)
    else:
        LOG.info(f'feed#{feed_id} url={feed.url} not modified by compare storys!')
    return dict(
        feed_id=feed_id,
        url=feed.url,
        status_code=status_code,
        num_storys=num_storys,
    )


@task(name='rssant.tasks.clean_user_feed')
def clean_user_feed():
    # 删除所有status=ERROR, 没有feed_id，并且入库时间超过2分钟的订阅
    num_deleted = UserFeed.delete_isolated_by_status(
        FeedStatus.ERROR, survival_seconds=2 * 60)
    LOG.info('delete {} status=ERROR and feed_id is NULL user feeds'.format(num_deleted))
    # 重试 status=UPDATING 超过10分钟的订阅
    user_feed_ids = UserFeed.query_isolated_ids_by_status(
        FeedStatus.UPDATING, survival_seconds=10 * 60)
    num_retry_updating = len(user_feed_ids)
    LOG.info('retry {} status=UPDATING and feed_id is NULL user feeds'.format(num_retry_updating))
    _retry_user_feeds(user_feed_ids)
    # 重试 status=PENDING 超过30分钟的订阅
    user_feed_ids = UserFeed.query_isolated_ids_by_status(
        FeedStatus.PENDING, survival_seconds=30 * 60)
    num_retry_pending = len(user_feed_ids)
    LOG.info('retry {} status=PENDING and feed_id is NULL user feeds'.format(num_retry_pending))
    _retry_user_feeds(user_feed_ids)
    return dict(
        num_deleted=num_deleted,
        num_retry_updating=num_retry_updating,
        num_retry_pending=num_retry_pending,
    )


@transaction.atomic
def _save_found(user_feed, parsed):
    url = _get_url(parsed.response)
    feed = Feed.get_first_by_url(url)
    if feed is None:
        feed = Feed()
    content_hash_base64 = compute_hash_base64(parsed.response.content)
    _save_feed(feed, parsed, content_hash_base64=content_hash_base64)
    _create_raw_feed(feed, parsed.response.status_code, parsed.response,
                     content_hash_base64=feed.content_hash_base64)
    user_feed_exists = UserFeed.get_first_by_user_and_feed(user_feed.user_id, feed.id)
    if user_feed_exists:
        LOG.info('UserFeed#{} user_id={} feed_id={} already exists'.format(
            user_feed_exists.id, user_feed.user_id, feed.id
        ))
        user_feed.status = FeedStatus.ERROR
        user_feed.save()
    else:
        user_feed.status = FeedStatus.READY
        user_feed.feed = feed
        user_feed.save()
    _save_storys(feed, parsed.entries)
    FeedUrlMap(source=user_feed.url, target=feed.url).save()
    if feed.link != user_feed.url:
        FeedUrlMap(source=feed.link, target=feed.url).save()
    if feed.url != user_feed.url:
        FeedUrlMap(source=feed.url, target=feed.url).save()


def _create_raw_feed(feed, status_code, response, content_hash_base64=None):
    raw_feed = RawFeed(feed=feed)
    raw_feed.url = feed.url
    raw_feed.status_code = status_code
    raw_feed.content_hash_base64 = content_hash_base64
    if response:
        raw_feed.encoding = response.encoding
        raw_feed.etag = _get_etag(response)
        raw_feed.last_modified = _get_last_modified(response)
        headers = {}
        for k, v in response.headers.items():
            headers[k.lower()] = v
        raw_feed.headers = headers
        raw_feed.set_content(response.content)
        raw_feed.content_length = len(response.content)
    raw_feed.save()
    return raw_feed


def _save_feed(feed, parsed, content_hash_base64=None, has_update=True):
    parsed_feed = parsed.feed
    res = parsed.response
    feed.url = _get_url(res)
    feed.title = shorten(parsed_feed["title"], 200)
    link = parsed_feed["link"]
    if not link.startswith('http'):
        # 有些link属性不是URL，用author_detail的href代替
        # 例如：'http://www.cnblogs.com/grenet/'
        author_detail = parsed_feed['author_detail']
        if author_detail:
            link = author_detail['href']
    feed.link = unquote(link)
    feed.author = shorten(parsed_feed["author"], 200)
    feed.icon = parsed_feed["icon"] or parsed_feed["logo"]
    feed.description = parsed_feed["description"] or parsed_feed["subtitle"]
    now = timezone.now()
    if has_update:
        feed.dt_updated = _get_dt_updated(parsed_feed, now)
    feed.dt_checked = feed.dt_synced = now
    feed.etag = _get_etag(res)
    feed.last_modified = _get_last_modified(res)
    feed.encoding = res.encoding
    feed.version = shorten(parsed.version, 200)
    feed.status = FeedStatus.READY
    feed.content_hash_base64 = content_hash_base64
    feed.save()
    return feed


def _save_storys(feed, entries):
    storys = []
    now = timezone.now()
    for data in entries:
        story = {}
        story['unique_id'] = shorten(_get_story_unique_id(data), 200)
        content = ''
        if data["content"]:
            content = "\n<br/>\n".join([x["value"] for x in data["content"]])
        if not content:
            content = data["description"]
        if not content:
            content = data["summary"]
        story['content'] = content
        summary = data["summary"]
        if not summary:
            summary = content
        summary = shorten(summary, width=300)
        story['summary'] = summary
        title = shorten(data["title"], 200)
        content_hash_base64 = compute_hash_base64(content, summary, title)
        story['title'] = title
        story['content_hash_base64'] = content_hash_base64
        story['link'] = unquote(data["link"])
        story['author'] = shorten(data["author"], 200)
        story['dt_published'] = _get_dt_published(data, now)
        story['dt_updated'] = _get_dt_updated(data, now)
        storys.append(story)
    num_modified, num_reallocate = Story.bulk_save_by_feed(feed.id, storys)
    LOG.info(
        'feed#%s save storys total=%s num_modified=%s num_reallocate=%s',
        feed.id, len(storys), num_modified, num_reallocate
    )
    return num_modified, len(storys)


def _get_etag(response):
    return response.headers.get("ETag")


def _get_last_modified(response):
    return response.headers.get("Last-Modified")


def _get_url(response):
    return unquote(response.url)


def _get_dt_published(data, default=None):
    t = data["published_parsed"] or data["updated_parsed"] or default
    if t and t > timezone.now():
        t = default
    return t


def _get_dt_updated(data, default=None):
    t = data["updated_parsed"] or data["published_parsed"] or default
    if t and t > timezone.now():
        t = default
    return t


def _get_story_unique_id(entry):
    unique_id = entry['id']
    if not unique_id:
        unique_id = entry['link']
    return unquote(unique_id)


def _read_response(feed):
    reader = FeedReader()
    status_code, response = reader.read(
        feed.url, etag=feed.etag, last_modified=feed.last_modified)
    return status_code, response


def _retry_user_feeds(user_feed_ids):
    tasks = []
    for user_feed_id in user_feed_ids:
        tasks.append(find_feed.s(user_feed_id))
    UserFeed.bulk_set_pending(user_feed_ids)
    celery.group(tasks).apply_async()
