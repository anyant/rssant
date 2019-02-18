from django.contrib.auth.decorators import login_required
from django_rest_validr import RestRouter, T, page_of, Cursor

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
@login_required
def story_list(
    request,
    feed_id: T.int.optional,
    detail: T.bool.default(False),
    data: T.bool.default(False),
    cursor: T.cursor.keys('id').optional,
    size: T.int.min(1).max(100).default(10),
) -> page_of(RssStorySchema):
    """Story list"""
    q = RssStory.objects.filter(user=request.user)
    if feed_id is not None:
        q = q.filter(feed_id=feed_id)
    if cursor:
        q = q.filter(id__gt=cursor.id)
    q = q.order_by('id')[:size]
    storys = q.all()
    storys = [x.to_dict(detail=detail, data=data) for x in storys]
    if len(storys) >= size:
        next = Cursor(id=storys[-1]['id'])
    else:
        next = None
    return dict(
        previous=cursor,
        next=next,
        size=len(storys),
        results=storys,
    )


@StoryView.get('story/<int:pk>')
@login_required
def story_get(
    request,
    pk: T.int,
    detail: T.bool.default(False),
    data: T.bool.default(False)
) -> RssStorySchema:
    """Story detail"""
    Story = RssStory.objects.get(user=request.user, pk=pk)
    return Story.to_dict(detail=detail, data=data)
