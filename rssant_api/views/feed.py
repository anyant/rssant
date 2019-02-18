from django.contrib.auth.decorators import login_required
from django_rest_validr import RestRouter, T

from rssant_api.models import RssFeed
from rssant_api.tasks import rss

RssFeedSchema = T.dict(
    id=T.int,
    user=T.dict(
        id=T.int,
        username=T.str.optional,
    ),
    url=T.url,
    link=T.str.optional,
    author=T.str.optional,
    icon=T.str.optional,
    description=T.str.optional,
    version=T.str.optional,
    title=T.str.optional,
    dt_created=T.datetime.optional,
    dt_updated=T.datetime.optional,
    encoding=T.str.optional,
    etag=T.str.optional,
    last_modified=T.str.optional,
    headers=T.dict.optional,
    data=T.dict.optional,
)

FeedView = RestRouter()


@FeedView.get('feed/')
@login_required
def feed_list(request, detail: T.bool.default(False)) -> T.list(RssFeedSchema):
    """Feed list"""
    feeds = RssFeed.objects.filter(user=request.user).all()
    feeds = [x.to_dict(detail=detail) for x in feeds]
    return feeds


@FeedView.get('feed/<int:pk>')
def feed_get(request, pk: T.int, detail: T.bool.default(False)) -> RssFeedSchema:
    """Feed detail"""
    feed = RssFeed.objects.get(pk=pk)
    return feed.to_dict(detail=detail)


@FeedView.post('feed/')
def feed_create(request, url: T.url) -> RssFeedSchema:
    feed = RssFeed.objects.create(user=request.user, url=url)
    feed.save()
    rss.find_feed.delay(feed_id=feed.id)
    return feed


@FeedView.put('feed/<int:pk>')
def feed_update(request, pk: T.int, url: T.url) -> RssFeedSchema:
    feed = RssFeed.objects.get(pk=pk)
    feed.url = url
    feed.save()
    rss.find_feed.delay(feed_id=feed.id)
    return feed


@FeedView.delete('feed/<int:pk>')
def feed_delete(request, pk: T.int):
    feed = RssFeed.objects.get(pk=pk)
    feed.delete()
