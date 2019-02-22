from django_rest_validr import RestRouter, T, pagination, Cursor

from rssant_api.models import RssStory

RssStorySchema = T.dict(
    id=T.int,
    user=T.dict(
        id=T.int,
        username=T.str.optional,
    ),
    feed=T.dict(
        id=T.int,
        url=T.url.optional,
        title=T.str.optional,
        link=T.str.optional,
    ),
    title=T.str.optional,
    link=T.str.optional,
    dt_created=T.datetime.optional,
    dt_updated=T.datetime.optional,
    summary=T.str.optional,
    content=T.str.optional,
    data=T.dict.optional,
)

StoryView = RestRouter()


@StoryView.get('story/')
def story_list(
    request,
    feed_id: T.int.optional,
    detail: T.bool.default(False),
    data: T.bool.default(False),
    cursor: T.cursor.object.keys('id').optional,
    size: T.int.min(1).max(100).default(10),
) -> pagination(RssStorySchema):
    """Story list"""
    q = RssStory.objects.filter(user=request.user)
    if feed_id is not None:
        q = q.filter(feed_id=feed_id)
    total = q.count()
    if detail:
        q = q.select_related('user', 'feed')
        q = q.defer('feed__data', 'feed__headers')
    else:
        q = q.defer('summary', 'content')
    if not data:
        q = q.defer('data')
    if cursor:
        q = q.filter(id__gt=cursor.id)
    storys = q.order_by('id')[:size].all()
    if detail:
        q = q.select_related('user', 'feed')
    storys = [x.to_dict(detail=detail, data=data) for x in storys]
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
    data: T.bool.default(False)
) -> RssStorySchema:
    """Story detail"""
    q = RssStory.objects
    if detail:
        q = q.select_related('user', 'feed')
        q = q.defer('feed__data', 'feed__headers')
    else:
        q = q.defer('summary', 'content')
    if not data:
        q = q.defer('data')
    Story = q.get(user=request.user, pk=pk)
    return Story.to_dict(detail=detail, data=data)
