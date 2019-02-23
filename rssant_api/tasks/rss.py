from urllib.parse import unquote

from celery import shared_task as task
from django.db import transaction
from django.utils import timezone

from feedlib import FeedFinder
from rssant.celery import LOG
from rssant_api.models import UserFeed, RawFeed, Feed, Story, FeedUrlMap, FeedStatus
from rssant_api.helper import shorten


def _save_raw_feed(feed, found):
    res = found.response
    raw_feed = RawFeed(feed=feed)
    raw_feed.url = feed.url
    raw_feed.encoding = res.encoding
    raw_feed.status_code = res.status_code
    raw_feed.etag = res.headers.get("ETag")
    raw_feed.last_modified = res.headers.get("Last-Modified")
    headers = {}
    for k, v in res.headers.items():
        headers[k.lower()] = v
    raw_feed.headers = headers
    raw_feed.content = res.content
    raw_feed.content_length = len(res.content)
    raw_feed.content_hash_method = None
    raw_feed.content_hash_value = None
    raw_feed.save()


def _save_feed_found(user_feed, found):
    feed_content = found.feed
    feed = Feed()
    feed.url = unquote(found.response.url)
    feed.title = feed_content["title"]
    link = feed_content["link"]
    if not link.startswith('http'):
        # 有些link属性不是URL，用author_detail的href代替
        # 例如：'http://www.cnblogs.com/grenet/'
        author_detail = feed_content['author_detail']
        if author_detail:
            link = author_detail['href']
    feed.link = unquote(link)
    feed.author = feed_content["author"]
    feed.icon = feed_content["icon"] or feed_content["logo"]
    feed.description = feed_content["description"] or feed_content["subtitle"]
    now = timezone.now()
    feed.dt_published = (
        feed_content["published_parsed"] or feed_content["updated_parsed"] or now
    )
    feed.dt_updated = (
        feed_content["updated_parsed"] or feed_content["published_parsed"] or now
    )
    feed.dt_checked = feed.dt_synced = now
    feed.etag = found.response.headers.get("ETag")
    feed.last_modified = found.response.headers.get("Last-Modified")
    feed.encoding = found.response.encoding
    feed.version = found.version
    feed.status = FeedStatus.READY
    feed.save()
    return feed


def _save_feed_entries(feed, found):
    for data in found.entries:
        unique_id = data['id']
        if not unique_id:
            unique_id = data['link']
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
        story = Story(
            feed=feed,
            unique_id=unique_id,
            title=data["title"],
            link=unquote(data["link"]),
            author=data["author"],
            dt_published=data["published_parsed"] or data["updated_parsed"] or now,
            dt_updated=data["updated_parsed"] or data["published_parsed"] or now,
            dt_synced=now,
            summary=summary,
            content=content,
        )
        story.save()


@transaction.atomic
def _save_feed(user_feed, found):
    user_feed.status = FeedStatus.READY
    feed = _save_feed_found(user_feed, found)
    user_feed.feed = feed
    user_feed.save()
    _save_raw_feed(feed, found)
    _save_feed_entries(feed, found)
    url_map = FeedUrlMap(source=user_feed.url, target=feed.url)
    url_map.save()


@task
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
    _save_feed(user_feed, found)
    return {'messages': messages}
