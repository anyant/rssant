import logging

import yarl
from validr import T
from django.db import transaction
from django.utils import timezone
from actorlib import actor, ActorContext

from rssant_feedlib import processor
from rssant_feedlib.reader import FeedResponseStatus
from rssant_feedlib.processor import StoryImageProcessor, story_html_to_text
from rssant_api.models import UserFeed, Feed, Story, FeedUrlMap, FeedStatus, FeedCreation
from rssant_common.image_url import encode_image_url
from rssant_common.actor_helper import django_context
from rssant.settings import ENV_CONFIG


LOG = logging.getLogger(__name__)

CHECK_FEED_SECONDS = ENV_CONFIG.check_feed_minutes * 60

StorySchema = T.dict(
    unique_id=T.str,
    title=T.str,
    content_hash_base64=T.str,
    author=T.str.optional,
    link=T.str.optional,
    dt_published=T.datetime.object.optional,
    dt_updated=T.datetime.object.optional,
    summary=T.str.optional,
    content=T.str.optional,
)

FeedSchema = T.dict(
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
    storys=T.list(StorySchema),
)


@actor('harbor_rss.update_feed_creation_status')
@django_context
def do_update_feed_creation_status(
    ctx: ActorContext,
    feed_creation_id: T.int,
    status: T.str,
):
    with transaction.atomic():
        feed_creation = FeedCreation.get_by_pk(feed_creation_id)
        feed_creation.status = FeedStatus.UPDATING
        feed_creation.save()


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
        feed_creation = FeedCreation.get_by_pk(feed_creation_id)
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
            feed = Feed(url=url, status=FeedStatus.READY, dt_updated=timezone.now())
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
    ctx.tell('harbor_rss.update_feed', dict(
        feed_id=feed.id,
        feed=feed_dict,
    ))


@actor('harbor_rss.update_feed')
@django_context
def do_update_feed(
    ctx: ActorContext,
    feed_id: T.int,
    feed: FeedSchema,
    is_refresh: T.bool.default(False),
):
    with transaction.atomic():
        feed_dict = feed
        storys = feed_dict.pop('storys')
        feed = Feed.get_by_pk(feed_id)
        for k, v in feed_dict.items():
            if v != '' and v is not None:
                setattr(feed, k, v)
        now = timezone.now()
        if not feed.dt_updated:
            feed.dt_updated = now
        feed.dt_checked = feed.dt_synced = now
        feed.save()
        for s in storys:
            if not s['dt_updated']:
                s['dt_updated'] = now
            if not s['dt_published']:
                s['dt_published'] = now
        modified_storys, num_reallocate = Story.bulk_save_by_feed(feed.id, storys)
        LOG.info(
            'feed#%s save storys total=%s num_modified=%s num_reallocate=%s',
            feed.id, len(storys), len(modified_storys), num_reallocate
        )
    need_fetch_story = _is_feed_need_fetch_storys(feed)
    for story in modified_storys:
        if need_fetch_story:
            ctx.tell('worker_rss.fetch_story', dict(
                url=story.link,
                story_id=str(story.id)
            ))
        elif story.link:
            _process_story_images(ctx, story, is_refresh)


def _is_feed_need_fetch_storys(feed):
    checkers = [processor.is_v2ex, processor.is_hacknews, processor.is_github, processor.is_pypi]
    for check in checkers:
        if check(feed.url):
            return False
    return True


def _process_story_images(ctx, story, is_refresh):
    story_text = story_html_to_text(story.content)
    if is_refresh or len(story_text) < 1000:
        processer = StoryImageProcessor(story.link, story.content)
        image_indexs = processer.parse()
        image_urls = {str(yarl.URL(x.value)) for x in image_indexs}
        LOG.info(f'found story#{story.id} {story.link} has {len(image_urls)} images')
        if image_urls:
            ctx.tell('worker_rss.detect_story_images', dict(
                story_id=story.id,
                story_url=story.link,
                image_urls=list(image_urls),
            ))


@actor('harbor_rss.update_story')
@django_context
def do_update_story(
    ctx: ActorContext,
    story_id: T.int,
    content: T.str,
    summary: T.str,
    url: T.url,
):
    with transaction.atomic():
        story = Story.objects.get(pk=story_id)
        story.link = url
        story.content = content
        story.summary = summary
        story.save()


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
    image_replaces = {}
    for img in images:
        if img['status'] in IMAGE_REFERER_DENY_STATUS:
            new_url_data = encode_image_url(img['url'], story_url)
            image_replaces[img['url']] = '/api/v1/image/{}'.format(new_url_data)
    LOG.info(f'detect story#{story_id} {story_url} '
             f'has {len(image_replaces)} referer deny images')
    with transaction.atomic():
        story = Story.objects.get(pk=story_id)
        processor = StoryImageProcessor(story_url, story.content)
        image_indexs = processor.parse()
        content = processor.process(image_indexs, image_replaces)
        story.content = content
        story.save()


@actor('harbor_rss.check_feed')
@django_context
def do_check_feed(ctx):
    feeds = Feed.take_outdated_feeds(CHECK_FEED_SECONDS)
    LOG.info('found {} feeds need sync'.format(len(feeds)))
    for feed in feeds:
        ctx.tell('worker_rss.sync_feed', dict(
            feed_id=feed['feed_id'],
            url=feed['url'],
        ))


@actor('harbor_rss.clean_feed_creation')
@django_context
def do_clean_feed_creation(ctx):
    # 删除所有入库时间超过2小时的订阅创建信息
    num_deleted = FeedCreation.delete_by_status(survival_seconds=2 * 60 * 60)
    LOG.info('delete {} old feed creations'.format(num_deleted))
    # 重试 status=UPDATING 超过10分钟的订阅
    feed_creation_ids = FeedCreation.query_ids_by_status(
        FeedStatus.UPDATING, survival_seconds=10 * 60)
    num_retry_updating = len(feed_creation_ids)
    LOG.info('retry {} status=UPDATING feed creations'.format(num_retry_updating))
    _retry_feed_creations(feed_creation_ids)
    # 重试 status=PENDING 超过30分钟的订阅
    feed_creation_ids = FeedCreation.query_ids_by_status(
        FeedStatus.PENDING, survival_seconds=CHECK_FEED_SECONDS)
    num_retry_pending = len(feed_creation_ids)
    LOG.info('retry {} status=PENDING feed creations'.format(num_retry_pending))
    _retry_feed_creations(feed_creation_ids)
    return dict(
        num_deleted=num_deleted,
        num_retry_updating=num_retry_updating,
        num_retry_pending=num_retry_pending,
    )


def _retry_feed_creations(feed_creation_ids):
    FeedCreation.bulk_set_pending(feed_creation_ids)
