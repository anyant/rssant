from django.utils import timezone
from django.db.models import Q
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
    is_readed=T.bool.default(False),
    dt_readed=T.datetime.optional,
    is_favorited=T.bool.default(False),
    dt_favorited=T.datetime.optional,
    summary=T.str.optional,
    content=T.str.optional,
)

StoryView = RestRouter()

STORY_DETAIL_FEILDS = ['story__summary', 'story__content']


@StoryView.get('story/')
def story_list(
    request,
    feed_id: T.int.optional,
    detail: T.bool.default(False),
    is_readed: T.bool.optional,
    is_favorited: T.bool.optional,
    cursor: T.cursor.object.keys('dt_updated, id').optional,
    size: T.int.min(1).max(100).default(10),
) -> pagination(StorySchema):
    """Story list"""
    user_feed_id = feed_id
    UserStory.sync_storys(user_id=request.user.id, user_feed_id=user_feed_id)
    q = UserStory.objects.filter(user=request.user)
    if user_feed_id is not None:
        q = q.filter(user_feed_id=user_feed_id)
    total = q.count()
    q = q.select_related('story')
    if not detail:
        q = q.defer(*STORY_DETAIL_FEILDS)
    if cursor:
        q_dt_gt = Q(story__dt_updated__gt=cursor.dt_updated)
        q_dt_eq = Q(story__dt_updated=cursor.dt_updated)
        q = q.filter(q_dt_gt | (q_dt_eq & Q(id__gt=cursor.id)))
    if is_readed is not None:
        q = q.filter(is_readed=is_readed)
    if is_favorited is not None:
        q = q.filter(is_favorited=is_favorited)
    storys = q.order_by('story__dt_updated', 'id')[:size].all()
    storys = [x.to_dict(detail=detail) for x in storys]
    if len(storys) >= size:
        dt_updated = storys[-1]['dt_updated']
        if dt_updated:
            dt_updated = dt_updated.isoformat()
        next = Cursor(id=storys[-1]['id'], dt_updated=dt_updated)
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
        q = q.defer(*STORY_DETAIL_FEILDS)
    story = q.get(user=request.user, pk=pk)
    return story.to_dict(detail=detail)


@StoryView.put('story/<int:pk>/readed')
def story_set_readed(
    request,
    pk: T.int,
    is_readed: T.bool.default(True),
) -> StorySchema:
    q = UserStory.objects.select_related('feed')
    q = q.defer(*STORY_DETAIL_FEILDS)
    story = q.get(user=request.user, pk=pk)
    story.is_readed = is_readed
    if is_readed:
        story.dt_readed = timezone.now()
    story.save()
    return story.to_dict()


@StoryView.put('story/all/readed')
def story_set_all_readed(
    request,
) -> T.dict(num_readed=T.int):
    num_readed = UserStory.objects.filter(user=request.user).update(is_readed=True)
    return dict(num_readed=num_readed)


@StoryView.put('story/<int:pk>/favorited')
def story_set_favorited(
    request,
    pk: T.int,
    is_favorited: T.bool.default(True),
) -> StorySchema:
    q = UserStory.objects.select_related('feed')
    q = q.defer(*STORY_DETAIL_FEILDS)
    story = q.get(user=request.user, pk=pk)
    story.is_favorited = is_favorited
    if is_favorited:
        story.dt_favorited = timezone.now()
    else:
        story.dt_favorited = None
    story.save()
    return story.to_dict()
