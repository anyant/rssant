import logging
import os.path

import celery
from django.http.response import HttpResponse
from django_rest_validr import RestRouter, T
from rest_framework.response import Response
from validr import Invalid
from xml.etree.ElementTree import ParseError
from xml.sax.saxutils import escape as xml_escape
from mako.template import Template

from rssant_feedlib.opml import parse_opml
from rssant_feedlib.bookmark import parse_bookmark
from rssant_api.models.errors import FeedExistError, FeedStoryOffsetError
from rssant_api.models import UnionFeed, FeedCreation
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
    is_ready=T.bool,
    feed_id=T.feed_unionid.optional,
    status=T.str,
    url=T.url,
    message=T.str.optional,
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
    feeds=T.list(FeedSchema).maxlen(5000),
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
        feeds=feeds,
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
        return Response({"message": "订阅不存在"}, status=400)
    return feed.to_dict()


@FeedView.post('feed/creation')
def feed_create(request, url: T.url.default_schema('http')) -> T.dict(
    is_ready=T.bool,
    feed=FeedSchema.optional,
    feed_creation=FeedCreationSchema.optional,
):
    try:
        feed, feed_creation = UnionFeed.create_by_url(url=url, user_id=request.user.id)
    except FeedExistError:
        return Response({'message': 'already exists'}, status=400)
    if feed_creation:
        rss.find_feed.delay(feed_creation.id)
    return dict(
        is_ready=bool(feed),
        feed=feed.to_dict() if feed else None,
        feed_creation=feed_creation.to_dict() if feed_creation else None,
    )


@FeedView.get('feed/creation/<int:pk>')
def feed_get_creation(request, pk: T.int, detail: T.bool.default(False)) -> FeedCreationSchema:
    try:
        feed_creation = FeedCreation.get_by_pk(pk, user_id=request.user.id, detail=detail)
    except FeedCreation.DoesNotExist:
        return Response({'message': 'feed creation does not exist'}, status=400)
    return feed_creation.to_dict(detail=detail)


@FeedView.get('feed/creation')
def feed_query_creation(request, detail: T.bool.default(False)) -> T.dict(
    total=T.int.min(0),
    size=T.int.min(0),
    feed_creations=T.list(FeedCreationSchema),
):
    feed_creations = FeedCreation.query_by_user(request.user.id, detail=detail)
    feed_creations = [x.to_dict() for x in feed_creations]
    return dict(
        total=len(feed_creations),
        size=len(feed_creations),
        feed_creations=feed_creations,
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
    try:
        UnionFeed.delete_by_id(feed_unionid)
    except FeedNotFoundError:
        return Response({"message": "订阅不存在"}, status=400)


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
    feeds = [x.to_dict() for x in feeds]
    feed_creations = [x.to_dict() for x in feed_creations]
    return dict(
        num_feeds=len(feeds),
        feeds=feeds,
        num_feed_creations=len(feed_creations),
        feed_creations=feed_creations,
    )


FeedImportResultSchema = T.dict(
    num_feeds=T.int.min(0).optional,
    feeds=T.list(FeedSchema).maxlen(5000),
    num_feed_creations=T.int.min(0).optional,
    feed_creations=T.list(FeedCreationSchema).maxlen(5000),
)


@FeedView.post('feed/opml')
def feed_import_opml(request) -> FeedImportResultSchema:
    """import feeds from OPML file"""
    text = _read_request_file(request)
    try:
        result = parse_opml(text)
    except (Invalid, ParseError) as ex:
        return Response({'message': str(ex)}, status=400)
    urls = list(sorted(set([x['url'] for x in result['items']])))
    return _create_feeds_by_urls(request.user, urls)


@FeedView.get('feed/opml')
def feed_export_opml(request, download: T.bool.default(False)):
    """export feeds to OPML file"""
    total, feeds, __ = UnionFeed.query_by_user(request.user.id)
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
def feed_import_bookmark(request) -> FeedImportResultSchema:
    """import feeds from bookmark file"""
    text = _read_request_file(request)
    urls = parse_bookmark(text)
    return _create_feeds_by_urls(request.user, urls, is_from_bookmark=True)
