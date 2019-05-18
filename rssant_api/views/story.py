from django.db import transaction

from django_rest_validr import RestRouter, T, pagination
from rest_framework.response import Response

from rssant_api.models import UserFeed, UserStory, FeedUnionId, StoryUnionId


StorySchema = T.dict(
    id=T.unionid('feed_id, user_feed_id, offset'),
    feed=T.dict(
        id=T.unionid('feed_id, user_feed_id'),
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
T_feed_unionid = T.unionid('feed_id, user_feed_id').object
T_story_unionid = T.unionid('feed_id, user_feed_id, offset').object


StoryView = RestRouter()

STORY_DETAIL_FEILDS = ['story__summary', 'story__content']


@StoryView.get('story/query')
def story_query_by_feed(
    request,
    feed_id: T_feed_unionid,
    offset: T.int.min(0).optional,
    size: T.int.min(1).max(100).default(10),
    detail: T.bool.default(False),
) -> pagination(StorySchema):
    """Story list"""
    user_feed_id = feed_id.user_feed_id
    try:
        total, offset, storys = UserStory.query_storys_by_feed(
            user_feed_id=user_feed_id, user_id=request.user.id,
            offset=offset, size=size, detail=detail)
    except UserFeed.DoesNotExist:
        return Response({"message": "feed does not exist"}, status=400)
    storys = [x.to_dict(detail=detail) for x in storys]
    for x in storys:
        syory_unionid = StoryUnionId(feed_id.feed_id, feed_id.user_feed_id, x['offset'])
        x.update(id=syory_unionid, feed=dict(id=feed_id), user=request.user)
    return dict(
        total=total,
        size=len(storys),
        results=storys,
    )


@StoryView.post('story/recent')
def story_query_recent_by_feed_s(
    request,
    feed_ids: T.list(T_feed_unionid),
    days: T.int.min(1).max(30).default(14),
    detail: T.bool.default(False),
) -> pagination(StorySchema):
    user_feed_ids = [x.user_feed_id for x in feed_ids]
    storys, feed_id_map = UserStory.query_recent_storys_by_feed_s(
        user_feed_ids=user_feed_ids, user_id=request.user.id,
        days=days, detail=detail)
    story_dicts = []
    for story in storys:
        d = story.to_dict(detail=detail)
        story_unionid = StoryUnionId(story.feed_id, feed_id_map[story.feed_id], story.offset)
        feed_unionid = FeedUnionId(story.feed_id, feed_id_map[story.feed_id])
        d.update(id=story_unionid, feed=dict(id=feed_unionid), user=request.user)
        story_dicts.append(d)
    return dict(
        total=len(story_dicts),
        size=len(story_dicts),
        results=story_dicts,
    )


@StoryView.get('story/<slug:feed_unionid>-<int:offset>')
def story_get_by_offset(
    request,
    feed_unionid: T_feed_unionid,
    offset: T.int.min(0).optional,
    detail: T.bool.default(False),
) -> StorySchema:
    """Story detail"""
    try:
        story = UserStory.get_story_by_offset(
            feed_unionid.user_feed_id, offset, user_id=request.user.id, detail=detail)
    except UserStory.DoesNotExist:
        return Response({"message": "does not exist"}, status=400)
    story = story.to_dict(detail=detail)
    story_unionid = StoryUnionId(feed_unionid.feed_id, feed_unionid.user_feed_id, story['offset'])
    story.update(id=story_unionid, feed=dict(id=feed_unionid), user=request.user)
    return story


@StoryView.get('story/favorited')
def story_query_favorited(
    request,
    detail: T.bool.default(False),
) -> pagination(StorySchema):
    """Query favorited storys"""
    user_storys = UserStory.query_by_user(user_id=request.user.id, is_favorited=True, detail=detail)
    user_storys = [x.to_dict(detail=detail) for x in user_storys]
    return dict(
        total=len(user_storys),
        size=len(user_storys),
        results=user_storys,
    )


@StoryView.get('story/watched')
def story_query_watched(
    request,
    detail: T.bool.default(False),
) -> pagination(StorySchema):
    """Query watched storys"""
    user_storys = UserStory.query_by_user(user_id=request.user.id, is_watched=True, detail=detail)
    user_storys = [x.to_dict(detail=detail) for x in user_storys]
    return dict(
        total=len(user_storys),
        size=len(user_storys),
        results=user_storys,
    )


@StoryView.put('story/<slug:feed_unionid>-<int:offset>/watched')
def story_set_watched(
    request,
    feed_unionid: T_feed_unionid,
    offset: T.int.min(0).optional,
    is_watched: T.bool.default(True),
) -> StorySchema:
    with transaction.atomic():
        user_story = UserStory.get_or_create_by_offset(
            feed_unionid.feed_id, offset, user_id=request.user.id)
        user_story.update_watched(is_watched)
    return user_story.to_dict()


@StoryView.put('story/<slug:feed_unionid>-<int:offset>/favorited')
def story_set_favorited(
    request,
    feed_unionid: T_feed_unionid,
    offset: T.int.min(0).optional,
    is_favorited: T.bool.default(True),
) -> StorySchema:
    with transaction.atomic():
        user_story = UserStory.get_or_create_by_offset(
            feed_unionid.feed_id, offset, user_id=request.user.id)
        user_story.update_favorited(is_favorited)
    return user_story.to_dict()
