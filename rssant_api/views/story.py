import logging
import socket

import requests
from rest_framework.response import Response

from django_rest_validr import RestRouter, T
from rssant_api.api_service import API_SERVICE
from rssant_api.export import build_favorites_zip, export_response
from rssant_api.models import (
    STORY_SERVICE,
    Feed,
    Story,
    StoryId,
    StoryInfo,
    UnionFeed,
    UnionStory,
    UserFeed,
    UserStory,
)
from rssant_api.models.errors import (
    FeedNotFoundError,
    FeedStoryOffsetError,
    StoryNotFoundError,
)
from rssant_api.models.helper import ConcurrentUpdateError
from rssant_api.models.story import StoryDetailSchema
from rssant_common.image_token import ImageToken
from rssant_config import CONFIG
from rssant_feedlib import FeedResponseStatus
from rssant_feedlib.fulltext import FulltextAcceptStrategy
from rssant_feedlib.processor import story_html_to_markdown

from .helper import check_unionid
from .publish import PublishView, is_only_publish, require_publish_user

LOG = logging.getLogger(__name__)

READ_BATCH_SIZE = 100


StorySchema = T.dict(
    id=T.story_unionid,
    feed=T.dict(
        id=T.feed_unionid,
        title=T.str.optional,
    ),
    offset=T.int,
    user=T.dict(
        id=T.int,
    ).optional,
    unique_id=T.str.optional,
    title=T.str.optional,
    link=T.str.optional,
    author=T.str.optional,
    image_url=T.str.optional,
    audio_url=T.str.optional,
    iframe_url=T.str.optional,
    enclosure_urls=T.list(T.str).optional,
    has_mathjax=T.bool.optional,
    sentence_count=T.int.optional,
    dt_published=T.datetime.object.optional.invalid_to_default,
    dt_updated=T.datetime.object.optional,
    dt_created=T.datetime.object.optional,
    dt_synced=T.datetime.object.optional,
    is_watched=T.bool.default(False),
    dt_watched=T.datetime.object.optional,
    is_read=T.bool.default(False),
    is_favorited=T.bool.default(False),
    dt_favorited=T.datetime.object.optional,
    summary=T.str.optional,
    content=T.str.optional,
    image_token=T.str.optional,
).slim

StoryResultSchema = T.dict(
    total=T.int.optional,
    size=T.int.optional,
    offset=T.int.optional,
    storys=T.list(StorySchema).maxlen(5000),
)

StoryView = RestRouter()
DeprecatedStoryView = StoryView  # TODO: 待废弃的接口

STORY_DETAIL_FEILDS = ['story__summary', 'story__content']


@DeprecatedStoryView.get('story/query')
@StoryView.post('story.query')
@PublishView.post('publish.story_query')
def story_query_by_feed(
    request,
    feed_id: T.feed_unionid.object,
    offset: T.int.min(0).optional,
    size: T.int.min(1).max(100).default(10),
    detail: StoryDetailSchema,
) -> StoryResultSchema:
    """Story list"""
    user = require_publish_user(request)
    check_unionid(user, feed_id)
    try:
        total, offset, storys = UnionStory.query_by_feed(
            feed_unionid=feed_id,
            offset=offset,
            size=size,
            detail=detail,
            only_publish=is_only_publish(request),
        )
    except FeedNotFoundError:
        return Response({"message": "feed does not exist"}, status=400)
    storys = [x.to_dict() for x in storys]
    return dict(
        total=total,
        offset=offset,
        size=len(storys),
        storys=storys,
    )


@DeprecatedStoryView.post('story/recent')
@StoryView.post('story.query_recent')
def story_query_recent(
    request,
    feed_ids: T.list(T.feed_unionid.object).optional,
    days: T.int.min(1).max(30).default(14),
    unread_only: T.bool.default(False),
    per_feed_limit: T.int.min(1).max(10).default(3),
    limit: T.int.min(1).max(200).default(100),
    detail: StoryDetailSchema,
) -> StoryResultSchema:
    check_unionid(request.user, feed_ids)
    total, storys = UnionStory.query_recent_by_user(
        user_id=request.user.id,
        feed_unionids=feed_ids,
        days=days,
        limit=limit,
        per_feed_limit=per_feed_limit,
        unread_only=unread_only,
        detail=detail,
    )
    storys = [x.to_dict() for x in storys]
    return dict(
        total=total,
        size=len(storys),
        storys=storys,
    )


@DeprecatedStoryView.post('story/query-batch')
@StoryView.post('story.query_batch')
def story_query_batch(
    request,
    storys: T.list(
        T.dict(
            feed_id=T.feed_unionid.object,
            offset=T.int.min(0),
            limit=T.int.min(1).max(10).default(1),
        )
    ),
    detail: StoryDetailSchema,
    content: T.bool.default(False),
    content_format: T.enum('html,markdown').default('html'),
) -> StoryResultSchema:
    feed_union_ids = [x['feed_id'] for x in storys]
    check_unionid(request.user, feed_union_ids)
    feed_ids = [x.feed_id for x in feed_union_ids]
    retention_map = dict(
        Feed.objects.filter(id__in=feed_ids).values_list('id', 'retention_offset')
    )
    story_keys = []
    for item in storys:
        feed_id = item['feed_id'].feed_id
        offset = item['offset']
        # 保留策略删掉了窗口之前的文章，从窗口起点开始取，
        # 避免老用户按旧 offset 拿到空结果
        offset = max(offset, retention_map.get(feed_id) or 0)
        for i in range(item['limit']):
            story_keys.append((feed_id, offset + i))
    storys = UnionStory.batch_get_by_feed_offset(
        story_keys=story_keys, user_id=request.user.id, detail=detail,
        content=content,
    )
    storys = [x.to_dict() for x in storys]
    if content and content_format == 'markdown':
        for x in storys:
            if x.get('content'):
                x['content'] = story_html_to_markdown(x['content'])
    return dict(
        total=len(storys),
        size=len(storys),
        storys=storys,
    )


@DeprecatedStoryView.get('story/<slug:feed_id>-<int:offset>')
@StoryView.post('story.get')
@PublishView.post('publish.story_get')
def story_get_by_offset(
    request,
    feed_id: T.feed_unionid.object,
    offset: T.int.min(0),
    detail: StoryDetailSchema,
    set_readed: T.bool.default(False),
    content_format: T.enum('html,markdown').default('html'),
) -> StorySchema:
    """Story detail"""
    user = require_publish_user(request)
    check_unionid(user, feed_id)
    only_publish = is_only_publish(request)
    try:
        story = UnionStory.get_by_feed_offset(
            feed_id,
            offset,
            detail=detail,
            only_publish=only_publish,
        )
    except StoryNotFoundError:
        return Response({"message": "does not exist"}, status=400)
    if (not only_publish) and set_readed:
        try:
            UnionFeed.set_story_offset(feed_id, offset + 1)
        except FeedStoryOffsetError as ex:
            return Response({'message': str(ex)}, status=400)
        except ConcurrentUpdateError as ex:
            LOG.error(f'ConcurrentUpdateError: story set_readed {ex}', exc_info=ex)
    image_token = ImageToken(
        referrer=story.link,
        feed=feed_id.feed_id,
        offset=offset,
    ).encode(secret=CONFIG.image_token_secret)
    ret = story.to_dict()
    if content_format == 'markdown' and ret.get('content'):
        ret['content'] = story_html_to_markdown(ret['content'])
    ret.update(image_token=image_token)
    return ret


@StoryView.get('story/export/favorited')
def story_export_favorited(request):
    """收藏文章打包导出"""
    return export_response(request.user, lambda: build_favorites_zip(request.user))


@DeprecatedStoryView.get('story/favorited')
@StoryView.post('story.query_favorited')
def story_query_favorited(
    request,
    detail: StoryDetailSchema,
) -> StoryResultSchema:
    """Query favorited storys"""
    storys = UnionStory.query_favorited(user_id=request.user.id, detail=detail)
    storys = [x.to_dict() for x in storys]
    return dict(
        total=len(storys),
        size=len(storys),
        storys=storys,
    )


@DeprecatedStoryView.get('story/watched')
@StoryView.post('story.query_watched')
def story_query_watched(
    request,
    detail: StoryDetailSchema,
) -> StoryResultSchema:
    """Query watched storys"""
    storys = UnionStory.query_watched(user_id=request.user.id, detail=detail)
    storys = [x.to_dict() for x in storys]
    return dict(
        total=len(storys),
        size=len(storys),
        storys=storys,
    )


@DeprecatedStoryView.put('story/<slug:feed_id>-<int:offset>/watched')
@StoryView.post('story.set_watched')
def story_set_watched(
    request,
    feed_id: T.feed_unionid.object,
    offset: T.int.min(0),
    is_watched: T.bool.default(True),
) -> StorySchema:
    check_unionid(request.user, feed_id)
    story = UnionStory.set_watched_by_feed_offset(
        feed_id, offset, is_watched=is_watched
    )
    return story.to_dict()


@StoryView.post('story.set_read')
def story_set_read(
    request,
    feed_id: T.feed_unionid.object,
    offset: T.int.min(0),
    is_read: T.bool.default(True),
) -> StorySchema:
    """Mark one story as read or unread"""
    check_unionid(request.user, feed_id)
    story = UnionStory.set_read_by_feed_offset(
        feed_id, offset, is_read=is_read
    )
    return story.to_dict()


@StoryView.post('story.query_unread_batch')
def story_query_unread_batch(
    request,
    storys: T.list(
        T.dict(
            feed_id=T.feed_unionid.object,
            limit=T.int.min(1).max(10).default(3),
        )
    ),
    detail: StoryDetailSchema,
) -> StoryResultSchema:
    """Query latest unread storys of each feed"""
    feed_union_ids = [x['feed_id'] for x in storys]
    check_unionid(request.user, feed_union_ids)
    user_id = request.user.id
    feed_ids = [x.feed_id for x in feed_union_ids]
    user_feeds = {
        x.feed_id: x for x in UserFeed.objects.filter(
            user_id=user_id, feed_id__in=feed_ids
        ).only('feed_id', 'story_offset')
    }
    feeds = {
        x.id: x for x in Feed.objects.filter(id__in=feed_ids).only(
            'id', 'retention_offset', 'total_storys')
    }
    union_storys = []
    for item in storys:
        feed_id = item['feed_id'].feed_id
        limit = item['limit']
        user_feed = user_feeds.get(feed_id)
        feed = feeds.get(feed_id)
        if not user_feed or not feed:
            continue
        begin = max(user_feed.story_offset, feed.retention_offset or 0)
        high = feed.total_storys - 1
        picked = []
        # 分批往前翻，直到凑够 limit 篇未读或到 story_offset
        while len(picked) < limit and high >= begin:
            size = min(READ_BATCH_SIZE, high - begin + 1)
            low = high - size + 1
            infos = list(
                StoryInfo.objects.filter(
                    pk__gte=StoryId.encode(feed_id, low),
                    pk__lte=StoryId.encode(feed_id, high),
                ).order_by('-pk')
            )
            if len(infos) < size:
                got_offsets = {x.offset for x in infos}
                fallback = Story.objects.filter(
                    feed_id=feed_id, offset__gte=low, offset__lte=high
                ).order_by('-offset')
                for story in fallback:
                    if story.offset not in got_offsets:
                        infos.append(STORY_SERVICE.to_common(story))
                infos.sort(key=lambda x: x.offset, reverse=True)
            offsets = [x.offset for x in infos]
            read_offsets = set(
                UserStory.objects.filter(
                    user_id=user_id, feed_id=feed_id,
                    is_read=True, offset__in=offsets,
                ).values_list('offset', flat=True)
            )
            picked.extend(x for x in infos if x.offset not in read_offsets)
            picked = picked[:limit]
            high = low - 1
        if not picked:
            continue
        picked_offsets = [x.offset for x in picked]
        user_storys = list(
            UserStory.objects.filter(
                user_id=user_id, feed_id=feed_id, offset__in=picked_offsets
            ).exclude(is_favorited=False, is_watched=False, is_read=False)
        )
        union_storys.extend(
            UnionStory._merge_storys(
                picked, user_storys, user_id=user_id, detail=detail
            )
        )
    storys = [x.to_dict() for x in union_storys]
    return dict(total=len(storys), size=len(storys), storys=storys)


@StoryView.put('story/<slug:feed_id>-<int:offset>/favorited')
@StoryView.post('story.set_favorited')
def story_set_favorited(
    request,
    feed_id: T.feed_unionid.object,
    offset: T.int.min(0),
    is_favorited: T.bool.default(True),
) -> StorySchema:
    check_unionid(request.user, feed_id)
    story = UnionStory.set_favorited_by_feed_offset(
        feed_id, offset, is_favorited=is_favorited
    )
    return story.to_dict()


T_ACCEPT = T.enum(','.join(FulltextAcceptStrategy.__members__))
_TIMEOUT_ERRORS = (socket.timeout, TimeoutError, requests.exceptions.Timeout)


@StoryView.post('story/fetch-fulltext')
@StoryView.post('story.fetch_fulltext')
def story_fetch_fulltext(
    request,
    feed_id: T.feed_unionid.object,
    offset: T.int.min(0),
) -> T.dict(
    feed_id=T.feed_unionid,
    offset=T.int.min(0),
    response_status=T.int,
    response_status_name=T.str,
    use_proxy=T.bool.optional,
    accept=T_ACCEPT.optional,
    story=StorySchema.optional,
):
    feed_unionid = feed_id
    check_unionid(request.user, feed_unionid)
    _, feed_id = feed_unionid
    use_proxy = None
    accept = None
    result = API_SERVICE.sync_story_fulltext(
        feed_id=feed_id,
        offset=offset,
        timeout=50,
    )
    response_status = result['response_status']
    accept = result['accept']
    story = None
    if accept != FulltextAcceptStrategy.REJECT.value:
        story = UnionStory.get_by_feed_offset(feed_unionid, offset, detail=True)
        story = story.to_dict()
    response_status_name = FeedResponseStatus.name_of(response_status)
    return dict(
        feed_id=feed_unionid,
        offset=offset,
        response_status=response_status,
        response_status_name=response_status_name,
        use_proxy=use_proxy,
        accept=accept,
        story=story,
    )
