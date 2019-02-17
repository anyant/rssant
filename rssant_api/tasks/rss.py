from urllib.parse import unquote
from celery import shared_task as task
from django.db import transaction

from feedlib import FeedFinder
from rssant.celery import LOG
from rssant_api.models import RssFeed, RssStory
from rssant_api.helper import shorten


def _save_feed_found(feed, found):
    feed_content = found.feed
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
    feed.dt_updated = (
        feed_content["updated_parsed"] or feed_content["published_parsed"]
    )
    feed.etag = found.response.headers.get("ETag")
    feed.last_modified = found.response.headers.get("Last-Modified")
    feed.encoding = found.response.encoding
    headers = {}
    for k, v in found.response.headers.items():
        headers[k.lower()] = v
    feed.headers = headers
    feed.version = found.version
    feed.data = feed_content
    feed.status = "ready"
    feed.save()


def _save_feed_entries(feed, entries):
    for data in entries:
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
        story = RssStory(
            feed_id=feed.id,
            user=feed.user,
            data=data,
            title=data["title"],
            link=unquote(data["link"]),
            dt_published=data["published_parsed"],
            dt_updated=data["updated_parsed"] or data["published_parsed"],
            summary=summary,
            content=content,
        )
        story.save()


@transaction.atomic
def _save_feed(feed, found):
    _save_feed_found(feed, found)
    _save_feed_entries(feed, found.entries)


@task
def find_feed(feed_id):
    messages = []

    def message_handler(msg):
        LOG.info(msg)
        messages.append(msg)

    feed = RssFeed.objects.get(pk=feed_id)
    start_url = feed.url
    finder = FeedFinder(start_url, message_handler=message_handler)
    found = finder.find()
    if not found:
        return {'messages': messages}
    _save_feed(feed, found)
    return {'messages': messages}
