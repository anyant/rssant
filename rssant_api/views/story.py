import logging
import socket

import requests
from rest_framework.response import Response

from django_rest_validr import RestRouter, T
from rssant_api.api_service import API_SERVICE
from rssant_api.models import UnionFeed, UnionStory
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

from .helper import check_unionid
from .publish import PublishView, is_only_publish, require_publish_user

LOG = logging.getLogger(__name__)


StorySchema = T.dict(
    id=T.story_unionid,
    feed=T.dict(
        id=T.feed_unionid,
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
    has_mathjax=T.bool.optional,
    sentence_count=T.int.optional,
    dt_published=T.datetime.object.optional.invalid_to_default,
    dt_updated=T.datetime.object.optional,
    dt_created=T.datetime.object.optional,
    dt_synced=T.datetime.object.optional,
    is_watched=T.bool.default(False),
    dt_watched=T.datetime.object.optional,
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
    detail: StoryDetailSchema,
) -> StoryResultSchema:
    check_unionid(request.user, feed_ids)
    storys = UnionStory.query_recent_by_user(
        user_id=request.user.id,
        feed_unionids=feed_ids,
        days=days,
        detail=detail,
    )
    storys = [x.to_dict() for x in storys]
    return dict(
        total=len(storys),
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
) -> StoryResultSchema:
    feed_union_ids = [x['feed_id'] for x in storys]
    check_unionid(request.user, feed_union_ids)
    story_keys = []
    for item in storys:
        feed_id = item['feed_id'].feed_id
        offset = item['offset']
        for i in range(item['limit']):
            story_keys.append((feed_id, offset + i))
    storys = UnionStory.batch_get_by_feed_offset(
        story_keys=story_keys, user_id=request.user.id, detail=detail
    )
    storys = [x.to_dict() for x in storys]
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
    offset: T.int.min(0).optional,
    detail: StoryDetailSchema,
    set_readed: T.bool.default(False),
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
    ret.update(image_token=image_token)
    return ret


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
    offset: T.int.min(0).optional,
    is_watched: T.bool.default(True),
) -> StorySchema:
    check_unionid(request.user, feed_id)
    story = UnionStory.set_watched_by_feed_offset(
        feed_id, offset, is_watched=is_watched
    )
    return story.to_dict()


@StoryView.put('story/<slug:feed_id>-<int:offset>/favorited')
@StoryView.post('story.set_favorited')
def story_set_favorited(
    request,
    feed_id: T.feed_unionid.object,
    offset: T.int.min(0).optional,
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
