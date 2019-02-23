from django_rest_validr import RestRouter, T, pagination, Cursor

from rssant_api.models import UserStory


StorySchema = T.dict(
    id=T.int,
    user=T.dict(
        id=T.int,
    ),
    feed=T.dict(
        id=T.int,
    ),
    unique_id=T.str.optional,
    title=T.str.optional,
    link=T.str.optional,
    dt_published=T.datetime.optional,
    dt_updated=T.datetime.optional,
    dt_created=T.datetime.optional,
    dt_synced=T.datetime.optional,
    summary=T.str.optional,
    content=T.str.optional,
)

StoryView = RestRouter()


@StoryView.get('story/')
def story_list(
    request,
    feed_id: T.int.optional,
    detail: T.bool.default(False),
    is_readed: T.bool.optional,
    is_favorited: T.bool.optional,
    cursor: T.cursor.object.keys('id').optional,
    size: T.int.min(1).max(100).default(10),
) -> pagination(StorySchema):
    """Story list"""
    user_feed_id = feed_id
    UserStory.sync_unreaded(user_id=request.user.id, user_feed_id=user_feed_id)
    q = UserStory.objects.filter(user=request.user)
    if user_feed_id is not None:
        q = q.filter(user_feed_id=user_feed_id)
    total = q.count()
    q = q.select_related('story')
    if not detail:
        q = q.defer('story__summary', 'story__content')
    if cursor:
        q = q.filter(id__gt=cursor.id)
    if is_readed is not None:
        q = q.filter(is_readed=is_readed)
    if is_favorited is not None:
        q = q.filter(is_favorited=is_favorited)
    storys = q.order_by('id')[:size].all()
    storys = [x.to_dict(detail=detail) for x in storys]
    if len(storys) >= size:
        next = Cursor(id=storys[-1]['id'])
    else:
        next = None
    return dict(
        previous=cursor,
        next=next,
        total=total,
        size=len(storys),
        results=storys,
    )


@StoryView.get('story/<int:pk>')
def story_get(
    request,
    pk: T.int,
    detail: T.bool.default(False),
) -> StorySchema:
    """Story detail"""
    q = UserStory.objects.select_related('feed')
    if not detail:
        q = q.defer('story__summary', 'story__content')
    story = q.get(user=request.user, pk=pk)
    return story.to_dict(detail=detail)
