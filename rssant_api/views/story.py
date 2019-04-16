from django_rest_validr import RestRouter, T, pagination

from rssant_api.models import UserStory


StorySchema = T.dict(
    feed=T.dict(
        id=T.int,
    ),
    offset=T.int,
    user=T.dict(
        id=T.int,
    ).optional,
    unique_id=T.str.optional,
    title=T.str.optional,
    link=T.str.optional,
    dt_published=T.datetime.optional,
    dt_updated=T.datetime.optional,
    dt_created=T.datetime.optional,
    dt_synced=T.datetime.optional,
    is_watched=T.bool.default(False),
    dt_watched=T.datetime.optional,
    is_favorited=T.bool.default(False),
    dt_favorited=T.datetime.optional,
    summary=T.str.optional,
    content=T.str.optional,
)

StoryView = RestRouter()

STORY_DETAIL_FEILDS = ['story__summary', 'story__content']


@StoryView.get('story/query')
def story_query_by_feed(
    request,
    feed_id: T.int,
    offset: T.int.min(0).optional,
    size: T.int.min(1).max(100).default(10),
    detail: T.bool.default(False),
) -> pagination(StorySchema):
    """Story list"""
    user_feed_id = feed_id
    total, offset, storys = UserStory.query_storys_by_feed(
        user_feed_id=user_feed_id, user_id=request.user.id,
        offset=offset, size=size, detail=detail)
    storys = [x.to_dict(detail=detail) for x in storys]
    for x in storys:
        x.update(feed=dict(id=user_feed_id))
    return dict(
        total=total,
        size=len(storys),
        results=storys,
    )


@StoryView.get('story/')
def story_get_by_feed_offset(
    request,
    feed_id: T.int,
    offset: T.int.min(0).optional,
    detail: T.bool.default(False),
) -> StorySchema:
    """Story detail"""
    user_story = UserStory.get_by_feed_offset(feed_id, offset, user_id=request.user.id)
    return user_story.to_dict(detail=detail)


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


@StoryView.put('story/<int:pk>/watched')
def story_set_watched(
    request,
    feed_id: T.int,
    offset: T.int.min(0).optional,
    is_watched: T.bool.default(True),
) -> StorySchema:
    user_story = UserStory.get_by_feed_offset(feed_id, offset, user_id=request.user.id)
    user_story.update_watched(is_watched)
    return user_story.to_dict()


@StoryView.put('story/<int:pk>/favorited')
def story_set_favorited(
    request,
    feed_id: T.int,
    offset: T.int.min(0).optional,
    is_favorited: T.bool.default(True),
) -> StorySchema:
    user_story = UserStory.get_by_feed_offset(feed_id, offset, user_id=request.user.id)
    user_story.update_favorited(is_favorited)
    return user_story.to_dict()
