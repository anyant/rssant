import logging
import os.path

import celery
from django.http.response import HttpResponse
from django_rest_validr import RestRouter, T, pagination
from rest_framework.response import Response
from validr import Invalid
from xml.etree.ElementTree import ParseError
from xml.sax.saxutils import escape as xml_escape
from mako.template import Template

from rssant_feedlib.opml import parse_opml
from rssant_feedlib.bookmark import parse_bookmark
from rssant_api.models.errors import FeedExistsError, FeedStoryOffsetError
from rssant_api.models import UnionFeed
from rssant_api.models.errors import FeedNotFoundError
from rssant_api.tasks import rss
from rssant.settings import BASE_DIR
from .helper import check_unionid

OPML_TEMPLATE_PATH = os.path.join(BASE_DIR, 'rssant_api', 'resources', 'opml.mako')


LOG = logging.getLogger(__name__)


FeedSchema = T.dict(
    id=T.feed_unionid,
    user=T.dict(
        id=T.int,
    ),
    status=T.str,
    url=T.url,
    link=T.str.optional,
    author=T.str.optional,
    icon=T.str.optional,
    description=T.str.optional,
    version=T.str.optional,
    title=T.str.optional,
    num_unread_storys=T.int.optional,
    total_storys=T.int.optional,
    dt_updated=T.datetime.object.optional,
    dt_created=T.datetime.object.optional,
    dt_checked=T.datetime.object.optional,
    dt_synced=T.datetime.object.optional,
    encoding=T.str.optional,
    etag=T.str.optional,
    last_modified=T.str.optional,
    content_hash_base64=T.str.optional,
    story_offset=T.int.min(0).optional,
    story_publish_period=T.int.min(0).optional,
    offset_early_story=T.int.min(0).optional,
    dt_early_story_published=T.datetime.object.optional,
    dt_latest_story_published=T.datetime.object.optional,
)

FeedCreationSchema = T.dict(
    id=T.int,
    user=T.dict(
        id=T.int,
    ),
    feed=T.dict(
        id=T.feed_unionid.optional,
    ).optional,
    status=T.str,
    url=T.url,
    dt_updated=T.datetime.object.optional,
    dt_created=T.datetime.object.optional,
)


FeedView = RestRouter()


@FeedView.get('feed/query')
@FeedView.post('feed/query')
def feed_query(
    request,
    hints: T.list(T.dict(id = T.feed_unionid.object, dt_updated = T.datetime.object)).optional,
    detail: T.bool.default(False)
) -> T.dict(
    total=T.int.optional,
    size=T.int.optional,
    results=T.list(FeedSchema).maxlen(5000),
    deleted_size=T.int.optional,
    deleted_ids=T.list(T.int),
):
    """Feed query"""
    check_unionid(request, [x['id'] for x in hints])
    total, feeds, deleted_ids = UnionFeed.query_by_user(
        user_id=request.user.id, hints=hints, detail=detail)
    feeds = [x.to_dict() for x in feeds]
    return dict(
        total=total,
        size=len(feeds),
        results=feeds,
        deleted_size=len(deleted_ids),
        deleted_ids=deleted_ids,
    )


@FeedView.get('feed/<slug:feed_unionid>')
def feed_get(request, feed_unionid: T.feed_unionid.object, detail: T.bool.default(False)) -> FeedSchema:
    """Feed detail"""
    check_unionid(request, feed_unionid)
    try:
        feed = UnionFeed.get_by_id(feed_unionid, detail=detail)
    except FeedNotFoundError:
        return Response({"message": "feed does not exist"}, status=400)
    return feed.to_dict()


@FeedView.post('feed/')
def feed_create(request, url: T.url.default_schema('http')) -> T.dict(
    feed=FeedSchema.optional,
    feed_creation=FeedCreationSchema.optional,
):
    try:
        feed, feed_creation = UnionFeed.create_by_url(url, user_id=request.user.id)
    except FeedExistsError:
        return Response({'message': 'already exists'}, status=400)
    if feed_creation:
        rss.find_feed.delay(feed_creation.id)
    return dict(
        feed=feed.to_dict(),
        feed_creation=feed_creation.to_dict(),
    )


@FeedView.put('feed/<slug:feed_unionid>')
def feed_update(request, feed_unionid: T.feed_unionid.object, title: T.str.optional) -> FeedSchema:
    check_unionid(request, feed_unionid)
    feed = UnionFeed.set_title(feed_unionid, title)
    return feed.to_dict()


@FeedView.put('feed/<slug:feed_unionid>/offset')
def feed_set_offset(request, feed_unionid: T.feed_unionid.object, offset: T.int.min(0).optional) -> FeedSchema:
    check_unionid(request, feed_unionid)
    try:
        feed = UnionFeed.set_story_offset(feed_unionid, offset)
    except FeedStoryOffsetError as ex:
        return Response({'message': str(ex)}, status=400)
    return feed.to_dict()


@FeedView.put('feed/all/readed')
def feed_set_all_readed(request, ids: T.list(T.feed_unionid.object).optional) -> T.dict(num_updated=T.int):
    check_unionid(request, ids)
    num_updated = UnionFeed.set_all_readed_by_user(user_id=request.user.id, ids=ids)
    return dict(num_updated=num_updated)


@FeedView.delete('feed/<slug:feed_unionid>')
def feed_delete(request, feed_unionid: T.feed_unionid.object):
    check_unionid(request, feed_unionid)
    UnionFeed.delete_by_id(feed_unionid)


def _read_request_file(request, name='file'):
    fileobj = request.data.get(name)
    if not fileobj:
        return Response(status=400)
    text = fileobj.read()
    if not isinstance(text, str):
        text = text.decode('utf-8')
    return text


def _create_feeds_by_urls(user, urls, is_from_bookmark=False):
    feeds, feed_creations = UnionFeed.create_by_url_s(urls=urls, user_id=user.id)
    find_feed_tasks = []
    for feed_creation in feed_creations:
        find_feed_tasks.append(rss.find_feed.s(feed_creation.id))
    # https://docs.celeryproject.org/en/latest/faq.html#does-celery-support-task-priorities
    # https://docs.celeryproject.org/en/latest/userguide/calling.html#routing-options
    # https://docs.celeryproject.org/en/latest/userguide/calling.html#advanced-options
    queue = 'bookmark' if is_from_bookmark else 'batch'
    celery.group(find_feed_tasks).apply_async(queue=queue)
    return feeds, feed_creations


@FeedView.post('feed/opml')
def feed_import_opml(request) -> pagination(FeedSchema, maxlen=5000):
    """import feeds from OPML file"""
    text = _read_request_file(request)
    try:
        result = parse_opml(text)
    except (Invalid, ParseError) as ex:
        return Response({'message': str(ex)}, status=400)
    urls = list(sorted(set([x['url'] for x in result['items']])))
    feeds, feed_creations = _create_feeds_by_urls(request.user, urls)
    feeds = [x.to_dict() for x in feeds]
    return dict(
        total=len(feeds),
        size=len(feeds),
        results=feeds
    )


@FeedView.get('feed/opml')
def feed_export_opml(request, download: T.bool.default(False)):
    """export feeds to OPML file"""
    feeds = UnionFeed.query_by_user(request.user.id)
    feeds = [x.to_dict() for x in feeds]
    for user_feed in feeds:
        for field in ['title', 'link', 'url', 'version']:
            user_feed[field] = xml_escape(user_feed[field] or '')
    tmpl = Template(filename=OPML_TEMPLATE_PATH)
    content = tmpl.render(feeds=feeds)
    response = HttpResponse(content, content_type='text/xml')
    if download:
        response['Content-Disposition'] = 'attachment;filename="rssant.opml"'
    return response


@FeedView.post('feed/bookmark')
def feed_import_bookmark(request) -> pagination(FeedSchema, maxlen=5000):
    """import feeds from bookmark file"""
    text = _read_request_file(request)
    urls = parse_bookmark(text)
    feeds, feed_creations = _create_feeds_by_urls(request.user, urls, is_from_bookmark=True)
    feeds = [x.to_dict() for x in feeds]
    return dict(
        total=len(feeds),
        size=len(feeds),
        results=feeds
    )
