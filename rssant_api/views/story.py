from django_rest_validr import RestRouter, T
from rest_framework.response import Response

from rssant_api.models.errors import FeedNotFoundError, StoryNotFoundError
from rssant_api.models import UnionStory
from .helper import check_unionid

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
    dt_published=T.datetime.object.optional,
    dt_updated=T.datetime.object.optional,
    dt_created=T.datetime.object.optional,
    dt_synced=T.datetime.object.optional,
    is_watched=T.bool.default(False),
    dt_watched=T.datetime.object.optional,
    is_favorited=T.bool.default(False),
    dt_favorited=T.datetime.object.optional,
    summary=T.str.optional,
    content=T.str.optional,
)

StoryResultSchema = T.dict(
    total=T.int.optional,
    size=T.int.optional,
    offset=T.int.optional,
    storys=T.list(StorySchema).maxlen(5000),
)

StoryView = RestRouter()

STORY_DETAIL_FEILDS = ['story__summary', 'story__content']


@StoryView.get('story/query')
def story_query_by_feed(
    request,
    feed_id: T.feed_unionid.object,
    offset: T.int.min(0).optional,
    size: T.int.min(1).max(100).default(10),
    detail: T.bool.default(False),
) -> StoryResultSchema:
    """Story list"""
    check_unionid(request, feed_id)
    try:
        total, offset, storys = UnionStory.query_by_feed(
            feed_unionid=feed_id, offset=offset, size=size, detail=detail)
    except FeedNotFoundError:
        return Response({"message": "feed does not exist"}, status=400)
    storys = [x.to_dict() for x in storys]
    return dict(
        total=total,
        offset=offset,
        size=len(storys),
        storys=storys,
    )


@StoryView.post('story/recent')
def story_query_recent(
    request,
    feed_ids: T.list(T.feed_unionid.object),
    days: T.int.min(1).max(30).default(14),
    detail: T.bool.default(False),
) -> StoryResultSchema:
    check_unionid(request, feed_ids)
    storys = UnionStory.query_recent_by_user(
        user_id=request.user.id,
        feed_unionids=feed_ids,
        days=days, detail=detail)
    storys = [x.to_dict() for x in storys]
    return dict(
        total=len(storys),
        size=len(storys),
        storys=storys,
    )


@StoryView.get('story/<slug:feed_unionid>-<int:offset>')
def story_get_by_offset(
    request,
    feed_unionid: T.feed_unionid.object,
    offset: T.int.min(0).optional,
    detail: T.bool.default(False),
) -> StorySchema:
    """Story detail"""
    check_unionid(request, feed_unionid)
    try:
        story = UnionStory.get_by_feed_offset(feed_unionid, offset, detail=detail)
    except StoryNotFoundError:
        return Response({"message": "does not exist"}, status=400)
    return story.to_dict()


@StoryView.get('story/favorited')
def story_query_favorited(
    request,
    detail: T.bool.default(False),
) -> StoryResultSchema:
    """Query favorited storys"""
    storys = UnionStory.query_favorited(user_id=request.user.id, detail=detail)
    storys = [x.to_dict() for x in storys]
    return dict(
        total=len(storys),
        size=len(storys),
        storys=storys,
    )


@StoryView.get('story/watched')
def story_query_watched(
    request,
    detail: T.bool.default(False),
) -> StoryResultSchema:
    """Query watched storys"""
    storys = UnionStory.query_watched(user_id=request.user.id, detail=detail)
    storys = [x.to_dict() for x in storys]
    return dict(
        total=len(storys),
        size=len(storys),
        storys=storys,
    )


@StoryView.put('story/<slug:feed_unionid>-<int:offset>/watched')
def story_set_watched(
    request,
    feed_unionid: T.feed_unionid.object,
    offset: T.int.min(0).optional,
    is_watched: T.bool.default(True),
) -> StorySchema:
    check_unionid(request, feed_unionid)
    story = UnionStory.set_watched_by_feed_offset(feed_unionid, offset, is_watched=is_watched)
    return story.to_dict()


@StoryView.put('story/<slug:feed_unionid>-<int:offset>/favorited')
def story_set_favorited(
    request,
    feed_unionid: T.feed_unionid.object,
    offset: T.int.min(0).optional,
    is_favorited: T.bool.default(True),
) -> StorySchema:
    check_unionid(request, feed_unionid)
    story = UnionStory.set_favorited_by_feed_offset(feed_unionid, offset, is_favorited=is_favorited)
    return story.to_dict()
