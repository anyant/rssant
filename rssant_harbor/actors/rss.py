import logging
import time
import random
from collections import defaultdict

import yarl
from validr import T
from django.db import transaction
from django.utils import timezone
from actorlib import actor, ActorContext

from rssant_feedlib import processor
from rssant_feedlib.reader import FeedResponseStatus
from rssant_feedlib.processor import StoryImageProcessor
from rssant_api.models import UserFeed, Feed, Story, FeedUrlMap, FeedStatus, FeedCreation, ImageInfo
from rssant_api.monthly_story_count import id_of_month, month_of_id
from rssant_common.image_url import encode_image_url
from rssant_common.actor_helper import django_context
from rssant_common.validator import compiler
from rssant_config import CONFIG


LOG = logging.getLogger(__name__)

CHECK_FEED_SECONDS = CONFIG.check_feed_minutes * 60

StorySchemaFields = dict(
    unique_id=T.str,
    title=T.str,
    content_hash_base64=T.str,
    author=T.str.optional,
    link=T.str.optional,
    has_mathjax=T.bool.optional,
    dt_published=T.datetime.object.optional.invalid_to_default,
    dt_updated=T.datetime.object.optional,
    summary=T.str.optional,
    content=T.str.optional,
)

StoryOutputSchemaFields = StorySchemaFields.copy()
StoryOutputSchemaFields.update(
    dt_published=T.datetime.optional,
    dt_updated=T.datetime.optional,
)

FeedSchemaFields = dict(
    url=T.url,
    title=T.str,
    content_hash_base64=T.str,
    link=T.str.optional,
    author=T.str.optional,
    icon=T.str.optional,
    description=T.str.optional,
    version=T.str.optional,
    dt_updated=T.datetime.object.optional,
    encoding=T.str.optional,
    etag=T.str.optional,
    last_modified=T.str.optional,
)

FeedOutputSchemaFields = FeedSchemaFields.copy()
FeedOutputSchemaFields.update(
    dt_updated=T.datetime.optional,
)

StorySchema = T.dict(**StorySchemaFields)
FeedSchema = T.dict(
    **FeedSchemaFields,
    storys=T.list(StorySchema),
)

StoryOutputSchema = T.dict(**StoryOutputSchemaFields)
FeedOutputSchema = T.dict(
    **FeedOutputSchemaFields,
    storys=T.list(StoryOutputSchema),
)

validate_feed_output = compiler.compile(FeedOutputSchema)


@actor('harbor_rss.update_feed_creation_status')
@django_context
def do_update_feed_creation_status(
    ctx: ActorContext,
    feed_creation_id: T.int,
    status: T.str,
):
    with transaction.atomic():
        FeedCreation.objects.filter(pk=feed_creation_id).update(status=status)


@actor('harbor_rss.save_feed_creation_result')
@django_context
def do_save_feed_creation_result(
    ctx: ActorContext,
    feed_creation_id: T.int,
    messages: T.list(T.str),
    feed: FeedSchema.optional,
):
    with transaction.atomic():
        feed_dict = feed
        try:
            feed_creation = FeedCreation.get_by_pk(feed_creation_id)
        except FeedCreation.DoesNotExist:
            LOG.warning(f'feed creation {feed_creation_id} not exists')
            return
        if feed_creation.status == FeedStatus.READY:
            LOG.info(f'feed creation {feed_creation_id} is ready')
            return
        feed_creation.message = '\n\n'.join(messages)
        feed_creation.dt_updated = timezone.now()
        if not feed_dict:
            feed_creation.status = FeedStatus.ERROR
            feed_creation.save()
            FeedUrlMap(source=feed_creation.url, target=FeedUrlMap.NOT_FOUND).save()
            return
        url = feed_dict['url']
        feed = Feed.get_first_by_url(url)
        if not feed:
            now = timezone.now()
            feed = Feed(
                url=url, status=FeedStatus.READY,
                dt_updated=now, dt_checked=now, dt_synced=now)
            feed.save()
        feed_creation.status = FeedStatus.READY
        feed_creation.feed_id = feed.id
        feed_creation.save()
        user_feed = UserFeed.objects.filter(user_id=feed_creation.user_id, feed_id=feed.id).first()
        if user_feed:
            LOG.info('UserFeed#{} user_id={} feed_id={} already exists'.format(
                user_feed.id, feed_creation.user_id, feed.id
            ))
        else:
            user_feed = UserFeed(
                user_id=feed_creation.user_id,
                feed_id=feed.id,
                is_from_bookmark=feed_creation.is_from_bookmark,
            )
            user_feed.save()
        FeedUrlMap(source=feed_creation.url, target=feed.url).save()
        if feed.url != feed_creation.url:
            FeedUrlMap(source=feed.url, target=feed.url).save()
    ctx.hope('harbor_rss.update_feed', dict(
        feed_id=feed.id,
        feed=validate_feed_output(feed_dict),
    ))


@actor('harbor_rss.update_feed')
@django_context
def do_update_feed(
    ctx: ActorContext,
    feed_id: T.int,
    feed: FeedSchema,
    is_refresh: T.bool.default(False).desc('Deprecated'),
):
    with transaction.atomic():
        feed_dict = feed
        storys = feed_dict.pop('storys')
        feed = Feed.get_by_pk(feed_id)
        is_feed_url_changed = feed.url != feed_dict['url']
        if is_feed_url_changed:
            target_feed = Feed.get_first_by_url(feed_dict['url'])
            if target_feed:
                LOG.info(f'merge feed#{feed.id} url={feed.url} into '
                         f'feed#{target_feed.id} url={target_feed.url}')
                target_feed.merge(feed)
                return
        for k, v in feed_dict.items():
            if v != '' and v is not None:
                setattr(feed, k, v)
        now = timezone.now()
        now_sub_30d = now - timezone.timedelta(days=30)
        if not feed.dt_updated:
            feed.dt_updated = now
        feed.dt_checked = feed.dt_synced = now
        feed.status = FeedStatus.READY
        feed.save()
        for s in storys:
            if not s['dt_updated']:
                s['dt_updated'] = now
            if not s['dt_published']:
                # set dt_published to now - 30d to avoid these storys
                # take over mushroom page, i.e. Story.query_recent_by_user
                s['dt_published'] = now_sub_30d
        modified_storys, num_reallocate = Story.bulk_save_by_feed(feed.id, storys)
        LOG.info(
            'feed#%s save storys total=%s num_modified=%s num_reallocate=%s',
            feed.id, len(storys), len(modified_storys), num_reallocate
        )
    feed.refresh_from_db()
    need_fetch_story = _is_feed_need_fetch_storys(feed)
    for story in modified_storys:
        if not story.link:
            continue
        if need_fetch_story and (not is_fulltext_story(feed, story)):
            ctx.tell('worker_rss.fetch_story', dict(
                url=story.link,
                story_id=str(story.id)
            ))
        else:
            _detect_story_images(ctx, story)


def is_productive_feed(monthly_story_count, date):
    """
    eg: news, forum, bbs, daily reports
    """
    year, month = date.year, date.month
    if not (1970 <= year <= 9999):
        return True
    month_id = id_of_month(year, month)
    count_18m = []
    for i in range(18):
        year_month = month_of_id(max(0, month_id - i))
        count_18m.insert(0, monthly_story_count.get(*year_month))
    if sum(count_18m) <= 0:
        return True
    freq_3m = max(count_18m[-3:]) / 30
    count_18m_non_zero = [x for x in count_18m if x > 0]
    freq_18m = sum(count_18m_non_zero) / len(count_18m_non_zero) / 30
    freq = max(freq_3m, freq_18m)
    if freq >= 1:
        return True
    return False


def is_fulltext_story(feed, story):
    """
    detect whether the full content is already in rss feed.

    see also: https://github.com/pictuga/morss/issues/27
    """
    if not story.content:
        return False
    if len(story.content) >= 2000:
        return True
    if not story.dt_published:
        return True
    if is_productive_feed(feed.monthly_story_count, story.dt_published):
        return True
    link_count = processor.story_link_count(story.content)
    if link_count >= 2:
        return True
    url_count = processor.story_url_count(story.content)
    if url_count >= 3:
        return True
    image_count = processor.story_image_count(story.content)
    if image_count >= 1:
        return True
    return False


def _is_feed_need_fetch_storys(feed):
    checkers = [processor.is_v2ex, processor.is_hacknews, processor.is_github, processor.is_pypi]
    for check in checkers:
        if check(feed.url):
            return False
    return True


def normalize_url(url):
    """
    >>> print(normalize_url('https://rss.anyant.com/^_^'))
    https://rss.anyant.com/%5E_%5E
    """
    return str(yarl.URL(url))


RSSANT_IMAGE_TAG = 'rssant=1'


def is_replaced_image(url):
    """
    >>> is_replaced_image('https://rss.anyant.com/123.jpg?rssant=1')
    True
    """
    return url and RSSANT_IMAGE_TAG in url


def _image_urls_of_indexs(image_indexs):
    image_urls = []
    for x in image_indexs:
        url = normalize_url(x.value)
        if not is_replaced_image(url):
            image_urls.append(url)
    return image_urls


def _detect_story_images(ctx, story):
    image_processor = StoryImageProcessor(story.link, story.content)
    image_urls = _image_urls_of_indexs(image_processor.parse())
    if not image_urls:
        return
    image_statuses = ImageInfo.batch_detect_images(image_urls)
    num_todo_image_urls = 0
    todo_url_roots = defaultdict(list)
    for url in image_urls:
        status = image_statuses.get(url)
        if status is None:
            num_todo_image_urls += 1
            url_root = ImageInfo.extract_url_root(url)
            todo_url_roots[url_root].append(url)
    LOG.info(
        f'story#{story.id} {story.link} has {len(image_urls)} images, '
        f'need detect {num_todo_image_urls} images '
        f'from {len(todo_url_roots)} url_roots'
    )
    if todo_url_roots:
        todo_urls = []
        for items in todo_url_roots.values():
            if len(items) > 3:
                todo_urls.extend(random.sample(items, 3))
            else:
                todo_urls.extend(items)
        ctx.hope('worker_rss.detect_story_images', dict(
            story_id=story.id,
            story_url=story.link,
            image_urls=list(set(todo_urls)),
        ))
    else:
        _replace_story_images(story.id)


@actor('harbor_rss.update_story')
@django_context
def do_update_story(
    ctx: ActorContext,
    story_id: T.int,
    content: T.str,
    summary: T.str,
    has_mathjax: T.bool.optional,
    url: T.url,
):
    with transaction.atomic():
        story = Story.objects.get(pk=story_id)
        story.link = url
        story.content = content
        story.summary = summary
        if has_mathjax is not None:
            story.has_mathjax = has_mathjax
        story.save()
    _detect_story_images(ctx, story)


IMAGE_REFERER_DENY_STATUS = set([
    400, 401, 403, 404,
    FeedResponseStatus.REFERER_DENY.value,
    FeedResponseStatus.REFERER_NOT_ALLOWED.value,
])


@actor('harbor_rss.update_story_images')
@django_context
def do_update_story_images(
    ctx: ActorContext,
    story_id: T.int,
    story_url: T.url,
    images: T.list(T.dict(
        url = T.url,
        status = T.int,
    ))
):
    # save image info
    url_root_status = {}
    for img in images:
        url_root = ImageInfo.extract_url_root(img['url'])
        value = (img['status'], img['url'])
        if url_root in url_root_status:
            url_root_status[url_root] = max(value, url_root_status[url_root])
        else:
            url_root_status[url_root] = value
    with transaction.atomic():
        image_info_objects = []
        for url_root, (status, url) in url_root_status.items():
            image_info_objects.append(ImageInfo(
                url_root=url_root,
                sample_url=url,
                referer=story_url,
                status_code=status,
            ))
        LOG.info(f'bulk create {len(image_info_objects)} ImageInfo objects')
        ImageInfo.objects.bulk_create(image_info_objects)
    _replace_story_images(story_id)


def _replace_story_images(story_id):
    story = Story.objects.get(pk=story_id)
    image_processor = StoryImageProcessor(story.link, story.content)
    image_indexs = image_processor.parse()
    image_urls = _image_urls_of_indexs(image_indexs)
    if not image_urls:
        return
    image_statuses = ImageInfo.batch_detect_images(image_urls)
    image_replaces = {}
    for url, status in image_statuses.items():
        if status in IMAGE_REFERER_DENY_STATUS:
            new_url_data = encode_image_url(url, story.link)
            image_replaces[url] = '/api/v1/image/{}?{}'.format(new_url_data, RSSANT_IMAGE_TAG)
    LOG.info(f'story#{story_id} {story.link} '
             f'replace {len(image_replaces)} referer deny images')
    # image_processor.process will (1) fix relative url (2) replace image url
    # call image_processor.process regardless of image_replaces is empty or not
    content = image_processor.process(image_indexs, image_replaces)
    story.content = content
    story.save()


@actor('harbor_rss.check_feed')
@django_context
def do_check_feed(ctx: ActorContext):
    rand_sec = random.random() * CHECK_FEED_SECONDS / 10
    outdate_seconds = CHECK_FEED_SECONDS + rand_sec
    feeds = Feed.take_outdated_feeds(outdate_seconds)
    expire_at = time.time() + outdate_seconds
    LOG.info('found {} feeds need sync'.format(len(feeds)))
    for feed in feeds:
        ctx.hope('worker_rss.sync_feed', dict(
            feed_id=feed['feed_id'],
            url=feed['url'],
        ), expire_at=expire_at)


@actor('harbor_rss.clean_feed_creation')
@django_context
def do_clean_feed_creation(ctx: ActorContext):
    # 删除所有入库时间超过24小时的订阅创建信息
    num_deleted = FeedCreation.delete_by_status(survival_seconds=24 * 60 * 60)
    LOG.info('delete {} old feed creations'.format(num_deleted))
    # 重试 status=UPDATING 超过30分钟的订阅
    feed_creation_id_urls = FeedCreation.query_id_urls_by_status(
        FeedStatus.UPDATING, survival_seconds=30 * 60)
    num_retry_updating = len(feed_creation_id_urls)
    LOG.info('retry {} status=UPDATING feed creations'.format(num_retry_updating))
    _retry_feed_creations(ctx, feed_creation_id_urls)
    # 重试 status=PENDING 超过60分钟的订阅
    feed_creation_id_urls = FeedCreation.query_id_urls_by_status(
        FeedStatus.PENDING, survival_seconds=60 * 60)
    num_retry_pending = len(feed_creation_id_urls)
    LOG.info('retry {} status=PENDING feed creations'.format(num_retry_pending))
    _retry_feed_creations(ctx, feed_creation_id_urls)
    return dict(
        num_deleted=num_deleted,
        num_retry_updating=num_retry_updating,
        num_retry_pending=num_retry_pending,
    )


def _retry_feed_creations(ctx: ActorContext, feed_creation_id_urls):
    feed_creation_ids = [id for (id, url) in feed_creation_id_urls]
    FeedCreation.bulk_set_pending(feed_creation_ids)
    expire_at = time.time() + 60 * 60
    for feed_creation_id, url in feed_creation_id_urls:
        ctx.hope('worker_rss.find_feed', dict(
            feed_creation_id=feed_creation_id,
            url=url,
        ), expire_at=expire_at)


@actor('harbor_rss.clean_by_retention')
@django_context
def do_clean_by_retention(ctx: ActorContext):
    retention = CONFIG.feed_story_retention
    while True:
        feeds = Feed.take_retention_feeds(retention=retention)
        if not feeds:
            break
        LOG.info('found {} feeds need clean by retention'.format(len(feeds)))
        for feed in feeds:
            feed_id = feed['feed_id']
            url = feed['url']
            n = Story.delete_by_retention(feed_id, retention=retention)
            LOG.info(f'deleted {n} storys of feed#{feed_id} {url} by retention')
