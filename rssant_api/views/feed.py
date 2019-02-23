from django_rest_validr import RestRouter, T, Cursor, pagination

from rssant_api.models import Feed, UserFeed, FeedUrlMap, FeedStatus
from rssant_api.tasks import rss

FeedSchema = T.dict(
    id=T.int,
    user=T.dict(
        id=T.int,
    ),
    status=T.str,
    url=T.url,
    link=T.str.optional,
    author=T.str.optional,
    icon=T.str.optional,
    description=T.str.optional,
    version=T.str.optional,
    title=T.str.optional,
    dt_updated=T.datetime.optional,
    dt_created=T.datetime.optional,
    dt_checked=T.datetime.optional,
    dt_synced=T.datetime.optional,
    encoding=T.str.optional,
    etag=T.str.optional,
    last_modified=T.str.optional,
    content_hash_method=T.str.optional,
    content_hash_value=T.str.optional,
)

FeedView = RestRouter()

FEED_DETAIL_FIELDS = [
    'feed__encoding',
    'feed__etag',
    'feed__last_modified',
    'feed__content_length',
    'feed__content_hash_method',
    'feed__content_hash_value',
]


@FeedView.get('feed/')
def feed_list(
    request,
    cursor: T.cursor.object.keys('id').optional,
    size: T.int.min(1).max(100).default(10),
    detail: T.bool.default(False)
) -> pagination(FeedSchema):
    """Feed list"""
    q = UserFeed.objects.filter(user=request.user)
    total = q.count()
    q = q.select_related('feed')
    if cursor:
        q = q.filter(id__gt=cursor.id)
    if not detail:
        q = q.defer(*FEED_DETAIL_FIELDS)
    feeds = q.order_by('id')[:size].all()
    feeds = [x.to_dict(detail=detail) for x in feeds]
    if len(feeds) >= size:
        next = Cursor(id=feeds[-1]['id'])
    else:
        next = None
    return dict(
        previous=cursor,
        next=next,
        total=total,
        size=size,
        results=feeds,
    )


@FeedView.get('feed/<int:pk>')
def feed_get(request, pk: T.int, detail: T.bool.default(False)) -> FeedSchema:
    """Feed detail"""
    q = UserFeed.objects.select_related('feed')
    if not detail:
        q = q.defer(*FEED_DETAIL_FIELDS)
    feed = q.get(user=request.user, pk=pk)
    return feed.to_dict(detail=detail)


@FeedView.post('feed/')
def feed_create(request, url: T.url) -> FeedSchema:
    target_url = FeedUrlMap.find_target(url)
    if target_url:
        feed = Feed.objects.get(url=target_url)
        user_feed = UserFeed(
            user=request.user, feed=feed, url=url, status=FeedStatus.READY.value)
        user_feed.save()
        return user_feed.to_dict(detail=True)
    else:
        user_feed = UserFeed(user=request.user, url=url)
        user_feed.save()
        rss.find_feed.delay(user_feed_id=user_feed.id)
        return user_feed.to_dict()


@FeedView.put('feed/<int:pk>')
def feed_update(request, pk: T.int, title: T.str.optional) -> FeedSchema:
    feed = UserFeed.objects.select_related('feed').get(user=request.user, pk=pk)
    feed.title = title
    feed.save()
    return feed.to_dict()


@FeedView.delete('feed/<int:pk>')
def feed_delete(request, pk: T.int):
    feed = UserFeed.objects.get(user=request.user, pk=pk)
    feed.delete()
