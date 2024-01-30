import logging

from django.http.response import HttpResponse
from rest_framework.response import Response

from django_rest_validr import RestRouter, T
from rssant_api.feed_helper import group_id_of, render_opml
from rssant_api.models import FeedCreation, FeedImportItem, UnionFeed
from rssant_api.models.errors import (
    FeedExistError,
    FeedNotFoundError,
    FeedStoryOffsetError,
)
from rssant_api.models.feed import FeedDetailSchema
from rssant_common.actor_client import scheduler
from rssant_common.helper import timer
from rssant_config import MAX_FEED_COUNT
from rssant_feedlib.importer import import_feed_from_text

from .errors import RssantAPIException
from .helper import check_unionid
from .publish import PublishView, is_publish_request, require_publish_user

LOG = logging.getLogger(__name__)


MAX_GROUP_NAME_LENGTH = 50

FeedSchema = T.dict(
    id=T.feed_unionid,
    user=T.dict(
        id=T.int,
    ),
    status=T.str,
    # TODO: limit url length in rssant-worker
    url=T.url.maxlen(4096).relaxed,
    link=T.str.optional,
    author=T.str.optional,
    icon=T.str.optional,
    description=T.str.optional,
    version=T.str.optional,
    title=T.str.maxlen(200).optional,
    group=T.str.maxlen(MAX_GROUP_NAME_LENGTH).optional,
    is_publish=T.bool.optional,
    warnings=T.str.optional,
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
    dryness=T.int.min(0).max(1000).optional,
    freeze_level=T.int.min(0).optional,
    use_proxy=T.bool.optional,
    response_status=T.int.optional,
    response_status_name=T.str.optional,
    dt_first_story_published=T.datetime.object.optional.invalid_to_default,
    dt_latest_story_published=T.datetime.object.optional.invalid_to_default,
).slim

FeedCreationSchema = T.dict(
    id=T.int,
    user=T.dict(
        id=T.int,
    ),
    is_ready=T.bool,
    feed_id=T.feed_unionid.optional,
    status=T.str,
    group=T.str.maxlen(MAX_GROUP_NAME_LENGTH).optional,
    url=T.url,
    message=T.str.optional,
    dt_updated=T.datetime.object.optional,
    dt_created=T.datetime.object.optional,
)


FeedView = RestRouter()


@FeedView.get('feed/query')
@FeedView.post('feed/query')
@PublishView.post('publish.feed_query')
def feed_query(
    request,
    hints: T.list(
        T.dict(
            id=T.feed_unionid.object,
            dt_updated=T.datetime.object,
        )
    )
    .maxlen(MAX_FEED_COUNT * 10)
    .optional,
    detail: FeedDetailSchema,
) -> T.dict(
    total=T.int.optional,
    size=T.int.optional,
    feeds=T.list(FeedSchema).maxlen(MAX_FEED_COUNT),
    deleted_size=T.int.optional,
    deleted_ids=T.list(T.feed_unionid).maxlen(MAX_FEED_COUNT),
):
    """Feed query, if user feed count exceed limit, only return limit feeds."""
    user = require_publish_user(request)
    if hints:
        # allow hints schema exceed feed count limit, but discard exceeded
        hints = hints[:MAX_FEED_COUNT]
        check_unionid(user, [x['id'] for x in hints])
    total, feeds, deleted_ids = UnionFeed.query_by_user(
        user_id=user.id,
        hints=hints,
        detail=detail,
        only_publish=is_publish_request(request),
    )
    feeds = [x.to_dict() for x in feeds]
    return dict(
        total=total,
        size=len(feeds),
        feeds=feeds,
        deleted_size=len(deleted_ids),
        deleted_ids=deleted_ids,
    )


@FeedView.get('feed/<slug:id>')
@PublishView.post('publish.feed_get')
def feed_get(
    request,
    id: T.feed_unionid.object,
    detail: FeedDetailSchema,
) -> FeedSchema:
    """Feed detail"""
    user = require_publish_user(request)
    check_unionid(user, id)
    try:
        feed = UnionFeed.get_by_id(
            id,
            detail=detail,
            only_publish=is_publish_request(request),
        )
    except FeedNotFoundError:
        return Response({"message": "订阅不存在"}, status=400)
    return feed.to_dict()


# TODO: fix flake8 check F821 in typing
# https://www.flake8rules.com/rules/F821.html
_SCHEMA_FEED_CREATE_URL = T.url.default_schema('http')


@FeedView.post('feed/creation')
def feed_create(
    request, url: _SCHEMA_FEED_CREATE_URL
) -> T.dict(
    is_ready=T.bool,
    feed=FeedSchema.optional,
    feed_creation=FeedCreationSchema.optional,
):
    """Deprecated, use feed_import instead."""
    try:
        feed, feed_creation = UnionFeed.create_by_url(
            url=url, user_id=request.user.id
        )
    except FeedExistError:
        return Response({'message': 'already exists'}, status=400)
    if feed_creation:
        scheduler.tell(
            'worker_rss.find_feed',
            dict(
                feed_creation_id=feed_creation.id,
                url=feed_creation.url,
            ),
        )
    return dict(
        is_ready=bool(feed),
        feed=feed.to_dict() if feed else None,
        feed_creation=feed_creation.to_dict() if feed_creation else None,
    )


@FeedView.get('feed/creation/<int:pk>')
def feed_get_creation(
    request, pk: T.int, detail: FeedDetailSchema
) -> FeedCreationSchema:
    try:
        feed_creation = FeedCreation.get_by_pk(
            pk, user_id=request.user.id, detail=detail
        )
    except FeedCreation.DoesNotExist:
        return Response(
            {'message': 'feed creation does not exist'}, status=400
        )
    return feed_creation.to_dict(detail=detail)


@FeedView.get('feed/creation')
def feed_query_creation(
    request,
    limit: T.int.min(10).max(MAX_FEED_COUNT).default(500),
    detail: FeedDetailSchema,
) -> T.dict(
    total=T.int.min(0),
    size=T.int.min(0),
    feed_creations=T.list(FeedCreationSchema).maxlen(MAX_FEED_COUNT),
):
    feed_creations = FeedCreation.query_by_user(
        request.user.id, limit=limit, detail=detail
    )
    feed_creations = [x.to_dict() for x in feed_creations]
    return dict(
        total=len(feed_creations),
        size=len(feed_creations),
        feed_creations=feed_creations,
    )


@FeedView.put('feed/<slug:feed_unionid>')
def feed_update(
    request,
    feed_unionid: T.feed_unionid.object,
    title: T.str.maxlen(200).optional,
) -> FeedSchema:
    """deprecated, use feed_set_title instead"""
    check_unionid(request.user, feed_unionid)
    feed = UnionFeed.set_title(feed_unionid, title)
    return feed.to_dict()


@FeedView.put('feed/set-title')
def feed_set_title(
    request,
    id: T.feed_unionid.object,
    title: T.str.maxlen(200).optional,
) -> FeedSchema:
    check_unionid(request.user, id)
    feed = UnionFeed.set_title(id, title)
    return feed.to_dict()


@FeedView.put('feed/set-group')
def feed_set_group(
    request,
    id: T.feed_unionid.object,
    group: T.str.maxlen(MAX_GROUP_NAME_LENGTH).optional,
) -> FeedSchema:
    check_unionid(request.user, id)
    feed = UnionFeed.set_group(id, group)
    return feed.to_dict()


@FeedView.put('feed/set-publish')
def feed_set_publish(
    request,
    id: T.feed_unionid.object,
    is_publish: T.bool,
) -> FeedSchema:
    check_unionid(request.user, id)
    feed = UnionFeed.set_publish(id, is_publish)
    return feed.to_dict()


@FeedView.put('feed/set-all-group')
def feed_set_all_group(
    request,
    ids: T.list(T.feed_unionid.object).maxlen(MAX_FEED_COUNT),
    group: T.str.maxlen(MAX_GROUP_NAME_LENGTH),
) -> T.dict(num_updated=T.int):
    check_unionid(request.user, ids)
    feed_ids = [x.feed_id for x in ids]
    num_updated = UnionFeed.set_all_group(
        user_id=request.user.id, feed_ids=feed_ids, group=group
    )
    return dict(num_updated=num_updated)


@FeedView.put('feed/<slug:feed_unionid>/offset')
def feed_set_offset(
    request,
    feed_unionid: T.feed_unionid.object,
    offset: T.int.min(0).optional,
) -> FeedSchema:
    check_unionid(request.user, feed_unionid)
    try:
        feed = UnionFeed.set_story_offset(feed_unionid, offset)
    except FeedStoryOffsetError as ex:
        return Response({'message': str(ex)}, status=400)
    return feed.to_dict()


@FeedView.put('feed/all/readed')
def feed_set_all_readed(
    request,
    ids: T.list(T.feed_unionid.object).maxlen(MAX_FEED_COUNT).optional,
) -> T.dict(num_updated=T.int):
    check_unionid(request.user, ids)
    num_updated = UnionFeed.set_all_readed_by_user(
        user_id=request.user.id, ids=ids
    )
    return dict(num_updated=num_updated)


@FeedView.delete('feed/<slug:feed_unionid>')
def feed_delete(request, feed_unionid: T.feed_unionid.object):
    check_unionid(request.user, feed_unionid)
    try:
        UnionFeed.delete_by_id(feed_unionid)
    except FeedNotFoundError:
        return Response({"message": "订阅不存在"}, status=400)


@FeedView.post('feed/all/delete')
def feed_delete_all(
    request,
    ids: T.list(T.feed_unionid.object).maxlen(MAX_FEED_COUNT).optional,
) -> T.dict(num_deleted=T.int):
    check_unionid(request.user, ids)
    num_deleted = UnionFeed.delete_all(user_id=request.user.id, ids=ids)
    return dict(num_deleted=num_deleted)


def _read_request_file(request, name='file'):
    fileobj = request.FILES.get(name)
    if not fileobj:
        raise RssantAPIException('file not received')
    text = fileobj.read()
    if not isinstance(text, str):
        try:
            text = text.decode('utf-8')
        except UnicodeError:
            raise RssantAPIException('file type or encoding invalid')
    return text, fileobj.name


def _create_feeds_by_imports(
    user,
    imports: list,
    group: str = None,
    is_from_bookmark=False,
):
    import_items = []
    for raw_item in imports:
        item_group = group
        if not item_group:
            item_group = raw_item.get('group')
        item_group = group_id_of(item_group)
        title = raw_item.get('title')
        item = FeedImportItem(
            url=raw_item['url'], title=title, group=item_group
        )
        import_items.append(item)
    result = UnionFeed.create_by_imports(imports=import_items, user_id=user.id)
    find_feed_tasks = []
    for feed_creation in result.feed_creations:
        find_feed_tasks.append(
            dict(
                dst='worker_rss.find_feed',
                content=dict(
                    feed_creation_id=feed_creation.id,
                    url=feed_creation.url,
                ),
            )
        )
    scheduler.batch_tell(find_feed_tasks)
    created_feeds = [x.to_dict() for x in result.created_feeds]
    feed_creations = [x.to_dict() for x in result.feed_creations]
    first_existed_feed = None
    if result.existed_feeds:
        first_existed_feed = result.existed_feeds[0].to_dict()
    return dict(
        total=result.total,
        num_created_feeds=len(result.created_feeds),
        num_existed_feeds=len(result.existed_feeds),
        num_feed_creations=len(result.feed_creations),
        first_existed_feed=first_existed_feed,
        created_feeds=created_feeds,
        feed_creations=feed_creations,
    )


FeedImportResultSchema = T.dict(
    total=T.int.min(0),
    num_created_feeds=T.int.min(0),
    num_existed_feeds=T.int.min(0),
    num_feed_creations=T.int.min(0),
    first_existed_feed=FeedSchema.optional,
    created_feeds=T.list(FeedSchema).maxlen(MAX_FEED_COUNT),
    feed_creations=T.list(FeedCreationSchema).maxlen(MAX_FEED_COUNT),
)


@FeedView.post('feed/opml')
def feed_import_opml(request) -> FeedImportResultSchema:
    """Deprecated. import feeds from OPML file"""
    return feed_import_file(request)


@FeedView.get('feed/opml')
@FeedView.get('feed/export/opml')
def feed_export_opml(request, download: T.bool.default(False)):
    """export feeds to OPML file"""
    total, user_feeds, __ = UnionFeed.query_by_user(request.user.id)
    content = render_opml(user_feeds)
    response = HttpResponse(content, content_type='text/xml')
    if download:
        response['Content-Disposition'] = 'attachment;filename="rssant.opml"'
    return response


@FeedView.post('feed/bookmark')
def feed_import_bookmark(request) -> FeedImportResultSchema:
    """Deprecated. import feeds from bookmark file"""
    return feed_import_file(request)


@FeedView.post('feed/import')
def feed_import(
    request,
    text: T.str,
    group: T.str.maxlen(MAX_GROUP_NAME_LENGTH).optional,
) -> FeedImportResultSchema:
    """从OPML/XML内容或含有链接的HTML或文本内容导入订阅"""
    with timer('Import-Feed-From-Text'):
        import_feeds = import_feed_from_text(text)
    if len(import_feeds) > MAX_FEED_COUNT:
        return Response({"message": "订阅数超过限制"}, status=400)
    is_from_bookmark = len(import_feeds) > 100
    return _create_feeds_by_imports(
        request.user,
        import_feeds,
        group=group,
        is_from_bookmark=is_from_bookmark,
    )


@FeedView.post('feed/import/file')
def feed_import_file(request) -> FeedImportResultSchema:
    """从OPML/XML/浏览器书签/含有链接的HTML或文本文件导入订阅"""
    text, filename = _read_request_file(request)
    group = request.GET.get('group')
    if group and len(group) > MAX_GROUP_NAME_LENGTH:
        raise RssantAPIException(
            f'group name length must <= {MAX_GROUP_NAME_LENGTH}'
        )
    return feed_import(request, text, group=group)
