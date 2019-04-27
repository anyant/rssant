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
from rssant_api.models.exceptions import FeedExistsException
from rssant_api.models import UserFeed, Story
from rssant_api.tasks import rss
from rssant.settings import BASE_DIR


OPML_TEMPLATE_PATH = os.path.join(BASE_DIR, 'rssant_api', 'resources', 'opml.mako')


LOG = logging.getLogger(__name__)


FeedSchema = T.dict(
    id=T.int,
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
    dt_first_story_published=T.datetime.object.optional,
    dt_last_story_published=T.datetime.object.optional,
)

FeedView = RestRouter()

FEED_DETAIL_FIELDS = [
    'feed__encoding',
    'feed__etag',
    'feed__last_modified',
    'feed__content_length',
    'feed__content_hash_base64',
]


@FeedView.get('feed/query')
@FeedView.post('feed/query')
def feed_query(
    request,
    hints: T.list(T.dict(id = T.int, dt_updated = T.datetime.object)).optional,
    detail: T.bool.default(False)
) -> T.dict(
    total=T.int.optional,
    size=T.int.optional,
    results=T.list(FeedSchema),
    deleted_size=T.int.optional,
    deleted_ids=T.list(T.int),
):
    """Feed query"""
    total, user_feeds, deleted_ids = UserFeed.query_by_user(
        user_id=request.user, hints=hints, detail=detail)
    feed_ids = [x.feed_id for x in user_feeds]
    dt_first_story_published_maps = Story.query_dt_first_story_published(feed_ids)
    dt_last_story_published_maps = Story.query_dt_last_story_published(feed_ids)
    user_feed_dicts = []
    for x in user_feeds:
        d = x.to_dict(detail=detail)
        dt_first_story_published = dt_first_story_published_maps.get(x.feed_id)
        d.update(dt_first_story_published=dt_first_story_published)
        dt_last_story_published = dt_last_story_published_maps.get(x.feed_id)
        d.update(dt_last_story_published=dt_last_story_published)
        user_feed_dicts.append(d)
    return dict(
        total=total,
        size=len(user_feed_dicts),
        results=user_feed_dicts,
        deleted_size=len(deleted_ids),
        deleted_ids=deleted_ids,
    )


@FeedView.get('feed/<int:pk>')
def feed_get(request, pk: T.int, detail: T.bool.default(False)) -> FeedSchema:
    """Feed detail"""
    user_feed = UserFeed.get_by_pk(pk, user_id=request.user.id, detail=detail)
    return user_feed.to_dict(detail=detail)


@FeedView.post('feed/')
def feed_create(request, url: T.url.default_schema('http')) -> FeedSchema:
    try:
        user_feed = UserFeed.create_by_url(url, user_id=request.user.id)
    except FeedExistsException:
        return Response({'message': 'already exists'}, status=400)
    if not user_feed.is_ready:
        rss.find_feed.delay(user_feed_id=user_feed.id)
    return user_feed.to_dict()


@FeedView.put('feed/<int:pk>')
def feed_update(request, pk: T.int, title: T.str.optional) -> FeedSchema:
    user_feed = UserFeed.get_by_pk(pk, user_id=request.user.id)
    user_feed.update_title(title)
    return user_feed.to_dict()


@FeedView.put('feed/<int:pk>/readed')
def feed_set_readed(request, pk: T.int, offset: T.int.min(0).optional) -> FeedSchema:
    user_feed = UserFeed.get_by_pk(pk, user_id=request.user.id, detail=True)
    if offset > user_feed.feed.total_storys:
        return Response({'message': 'offset too large'}, status=400)
    user_feed.update_story_offset(offset)
    return user_feed.to_dict()


@FeedView.put('feed/all/readed')
def feed_set_all_readed(request, ids: T.list(T.int).optional) -> T.dict(num_updated=T.int):
    num_updated = UserFeed.set_all_readed_by_user(user_id=request.user.id, ids=ids)
    return dict(num_updated=num_updated)


@FeedView.delete('feed/<int:pk>')
def feed_delete(request, pk: T.int):
    UserFeed.delete_by_pk(pk, user_id=request.user.id)


def _read_request_file(request, name='file'):
    fileobj = request.data.get(name)
    if not fileobj:
        return Response(status=400)
    text = fileobj.read()
    if not isinstance(text, str):
        text = text.decode('utf-8')
    return text


def _create_feeds_by_urls(user, urls, is_from_bookmark=False):
    user_feeds = UserFeed.create_by_url_s(urls, user_id=user.id)
    readys = []
    find_feed_tasks = []
    for user_feed in user_feeds:
        if not user_feed.is_ready:
            find_feed_tasks.append(rss.find_feed.s(user_feed.id))
        else:
            readys.append(user_feed)
    celery.group(find_feed_tasks).apply_async()
    return user_feeds, readys


@FeedView.post('feed/opml')
def feed_import_opml(request) -> pagination(FeedSchema, maxlen=5000):
    """import feeds from OPML file"""
    text = _read_request_file(request)
    try:
        result = parse_opml(text)
    except (Invalid, ParseError) as ex:
        return Response({'message': str(ex)}, status=400)
    urls = list(sorted(set([x['url'] for x in result['items']])))
    user_feeds, readys = _create_feeds_by_urls(request.user, urls)
    readys = [x.to_dict() for x in readys]
    return dict(
        total=len(user_feeds),
        size=len(readys),
        results=readys
    )


@FeedView.get('feed/opml')
def feed_export_opml(request, download: T.bool.default(False)):
    """export feeds to OPML file"""
    user_feeds = UserFeed.query_by_user(request.user.id, show_pending=True)
    user_feeds = [x.to_dict() for x in user_feeds]
    for user_feed in user_feeds:
        for field in ['title', 'link', 'url', 'version']:
            user_feed[field] = xml_escape(user_feed[field] or '')
    tmpl = Template(filename=OPML_TEMPLATE_PATH)
    content = tmpl.render(feeds=user_feeds)
    response = HttpResponse(content, content_type='text/xml')
    if download:
        response['Content-Disposition'] = 'attachment;filename="rssant.opml"'
    return response


@FeedView.post('feed/bookmark')
def feed_import_bookmark(request) -> pagination(FeedSchema, maxlen=5000):
    """import feeds from bookmark file"""
    text = _read_request_file(request)
    urls = parse_bookmark(text)
    user_feeds, readys = _create_feeds_by_urls(request.user, urls, is_from_bookmark=True)
    readys = [x.to_dict() for x in readys]
    return dict(
        total=len(user_feeds),
        size=len(readys),
        results=readys
    )
