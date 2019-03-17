import logging

from django.db import connection, transaction
from django.db.models import Q
from django_rest_validr import RestRouter, T, Cursor, pagination
from rest_framework.response import Response
from validr import Invalid
from xml.etree.ElementTree import ParseError
from feedlib.opml import parse_opml

from rssant_api.models import Feed, UserFeed, FeedUrlMap, FeedStatus, UserStory
from rssant_api.tasks import rss


LOG = logging.getLogger(__name__)


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
    num_unread_storys=T.int.optional,
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
    cursor: T.cursor.object.keys('id, dt_updated').optional,
    size: T.int.min(1).max(100).default(10),
    detail: T.bool.default(False)
) -> pagination(FeedSchema):
    """Feed list"""
    q = UserFeed.objects.filter(user=request.user)
    total = q.count()
    q = q.select_related('feed')
    if cursor and cursor.dt_updated:
        q_dt_lt = Q(feed__dt_updated__lt=cursor.dt_updated)
        q_dt_eq = Q(feed__dt_updated=cursor.dt_updated)
        q = q.filter(q_dt_lt | (q_dt_eq & Q(id__lt=cursor.id)))
    if not detail:
        q = q.defer(*FEED_DETAIL_FIELDS)
    feeds = q.order_by('-feed__dt_updated', '-id')[:size].all()
    feeds = [x.to_dict(detail=detail) for x in feeds]
    user_feed_ids = [x['id'] for x in feeds]
    feed_unread_stats = _get_feed_unread_stats(request.user.id, user_feed_ids)
    for feed in feeds:
        feed['num_unread_storys'] = feed_unread_stats.get(feed['id'])
    if len(feeds) >= size:
        dt_updated = feeds[-1]['dt_updated']
        next = Cursor(id=feeds[-1]['id'], dt_updated=dt_updated)
    else:
        next = None
    return dict(
        previous=cursor,
        next=next,
        total=total,
        size=size,
        results=feeds,
    )


def _get_feed_unread_stats(user_id, user_feed_ids):
    UserStory.sync_storys(user_id=user_id)
    sql = """
    SELECT user_feed_id, count(1) AS count
    FROM rssant_api_userstory AS userstory
    WHERE user_id=%s AND user_feed_id = ANY(%s)
        AND (is_readed IS NULL OR NOT is_readed)
    GROUP BY user_feed_id
    """
    feed_unread_stats = {}
    with connection.cursor() as cursor:
        cursor.execute(sql, [user_id, user_feed_ids])
        for user_feed_id, count in cursor.fetchall():
            feed_unread_stats[user_feed_id] = count
    return feed_unread_stats


@FeedView.get('feed/<int:pk>')
def feed_get(request, pk: T.int, detail: T.bool.default(False)) -> FeedSchema:
    """Feed detail"""
    q = UserFeed.objects.select_related('feed')
    if not detail:
        q = q.defer(*FEED_DETAIL_FIELDS)
    feed = q.get(user=request.user, pk=pk)
    return feed.to_dict(detail=detail)


@FeedView.post('feed/')
def feed_create(request, url: T.url.tolerant) -> FeedSchema:
    feed = None
    target_url = FeedUrlMap.find_target(url)
    if target_url:
        feed = Feed.objects.filter(url=target_url).first()
    if feed:
        user_feed = UserFeed.objects.filter(user=request.user, feed=feed).first()
        if user_feed:
            return Response({'message': 'already exists'}, status=400)
        user_feed = UserFeed(user=request.user, feed=feed, url=url, status=FeedStatus.READY)
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


@FeedView.put('feed/<int:pk>/readed')
def feed_readed(request, pk: T.int) -> T.dict(num_readed=T.int):
    num_readed = UserStory.objects\
        .filter(user=request.user, user_feed_id=pk)\
        .update(is_readed=True)
    return dict(num_readed=num_readed)


@FeedView.delete('feed/<int:pk>')
def feed_delete(request, pk: T.int):
    feed = UserFeed.objects.get(user=request.user, pk=pk)
    feed.delete()


@FeedView.post('feed/opml/')
def feed_import_opml(request) -> T.dict(feeds=T.list(FeedSchema)):
    """import feeds from OPML file"""
    fileobj = request.data.get('file')
    if not fileobj:
        return Response(status=400)
    text = fileobj.read()
    if not isinstance(text, str):
        text = text.decode('utf-8')
    try:
        result = parse_opml(text)
    except (Invalid, ParseError) as ex:
        return Response({'message': str(ex)}, status=400)
    urls = list(sorted(set([x['url'] for x in result['items']])))
    # 批量预查询，减少SQL查询数量，显著提高性能
    feeds = []
    find_feed_ids = []
    url_map = FeedUrlMap.find_all_target(urls)
    feed_map = {}
    found_feeds = Feed.objects.filter(url__in=set(url_map.values())).all()
    for x in found_feeds:
        feed_map[x.url] = x
    user_feed_map = {}
    found_user_feeds = UserFeed.objects.filter(user=request.user, feed__in=found_feeds).all()
    for x in found_user_feeds:
        user_feed_map[x.feed_id] = x
    with transaction.atomic():
        for url in urls:
            feed = feed_map.get(url_map.get(url))
            if feed:
                if feed.id in user_feed_map:
                    continue
                user_feed = UserFeed(user=request.user, feed=feed, url=url, status=FeedStatus.READY)
                user_feed.save()
                feeds.append(user_feed.to_dict(detail=True))
            else:
                user_feed = UserFeed(user=request.user, url=url)
                user_feed.save()
                find_feed_ids.append(user_feed.id)
                feeds.append(user_feed.to_dict())
    for user_feed_id in find_feed_ids:
        rss.find_feed.delay(user_feed_id=user_feed_id)
    return dict(feeds=feeds)
