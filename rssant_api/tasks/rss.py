from urllib.parse import unquote

from celery import shared_task as task
from django.db import transaction
from django.utils import timezone
import requests.exceptions

from feedlib import FeedFinder, FeedReader, FeedParser
from rssant.celery import LOG
from rssant_api.models import UserFeed, RawFeed, Feed, Story, FeedUrlMap, FeedStatus
from rssant_api.helper import shorten


def _get_etag(response):
    return response.headers.get("ETag")


def _get_last_modified(response):
    return response.headers.get("Last-Modified")


def _get_url(response):
    return unquote(response.url)


def _get_dt_published(data):
    return data["published_parsed"] or data["updated_parsed"] or None


def _get_dt_updated(data):
    return data["updated_parsed"] or data["published_parsed"] or None


def _get_story_unique_id(entry):
    unique_id = entry['id']
    if not unique_id:
        unique_id = entry['link']
    return unique_id


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


def _create_feed(parsed):
    return _save_feed(Feed(), parsed)


def _save_storys(feed, entries):
    unique_ids = [_get_story_unique_id(x) for x in entries]
    storys = {}
    q = Story.objects.filter(feed_id=feed.id, unique_id__in=unique_ids)
    for story in q.all():
        storys[story.unique_id] = story
    for data in entries:
        unique_id = _get_story_unique_id(data)
        if unique_id in storys:
            story = storys[unique_id]
            LOG.info(f'update story feed_id={feed.id} unique_id={unique_id}')
        else:
            LOG.info(f'create story feed_id={feed.id} unique_id={unique_id}')
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
        story.save()
    return list(storys.values())


@transaction.atomic
def _save_found(user_feed, found):
    user_feed.status = FeedStatus.READY
    feed = _create_feed(found)
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
        return {'messages': messages}
    _save_found(user_feed, found)
    return {'messages': messages}


@task(name='rssant.tasks.check_feed')
def check_feed(seconds=300):
    dt_before = timezone.now() - timezone.timedelta(seconds=seconds)
    q = Feed.objects\
        .filter(dt_checked__lt=dt_before)\
        .exclude(status__in=(FeedStatus.PENDING, FeedStatus.UPDATING))\
        .only('id', 'status')
    feeds = list(q.all())
    feed_ids = [x.id for x in feeds]
    for feed in feeds:
        feed.status = FeedStatus.PENDING
        feed.save()
        sync_feed.delay(feed_id=feed.id)
    return feed_ids


@task(name='rssant.tasks.sync_feed')
def sync_feed(feed_id):
    feed = Feed.objects.get(pk=feed_id)
    feed.status = FeedStatus.UPDATING
    feed.save()
    reader = FeedReader()
    LOG.info(f'read feed#{feed_id} url={feed.url}')
    try:
        response = reader.read(feed.url, etag=feed.etag, last_modified=feed.last_modified)
    except requests.exceptions.RequestException:
        feed.status = FeedStatus.ERROR
        feed.save()
        raise
    if 200 <= response.status_code <= 299:
        __, hash_value = feed.compute_content_hash(response.content)
        if hash_value == feed.content_hash_value:
            LOG.info(f'feed#{feed_id} url={feed.url} not changed')
            parsed = None
        else:
            LOG.info(f'parse feed#{feed_id} url={feed.url}')
            parsed = FeedParser.parse_response(response)
    else:
        LOG.info(f'feed#{feed_id} url={feed.url} response={response.status_code}')
        parsed = None
    with transaction.atomic():
        raw_feed = _create_raw_feed(feed, response)
        if parsed:
            _save_feed(feed, parsed)
            _update_feed_content_info(feed, raw_feed)
            _save_storys(feed, parsed.entries)
        feed.status = FeedStatus.READY
        feed.save()
