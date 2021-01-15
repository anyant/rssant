import logging
import time
import random
import socket

import requests
from validr import T
from django.db import transaction
from django.utils import timezone
from actorlib import actor, ActorContext

from rssant_feedlib import processor
from rssant_feedlib import FeedResponseStatus
from rssant_feedlib.fulltext import (
    split_sentences, is_fulltext_content, StoryContentInfo,
    decide_accept_fulltext, FulltextAcceptStrategy,
)
from rssant_api.models import UserFeed, Feed, STORY_SERVICE, CommonStory, FeedUrlMap, FeedStatus, FeedCreation
from rssant_api.helper import reverse_url
from rssant_common.actor_helper import (
    django_context, profile_django_context, log_django_context_metric)
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
    image_url=T.url.optional,
    iframe_url=T.url.optional,
    audio_url=T.url.optional,
    has_mathjax=T.bool.optional,
    dt_published=T.datetime.object.optional.invalid_to_default,
    dt_updated=T.datetime.object.optional,
    summary=T.str.optional,
    content=T.str.optional,
    sentence_count=T.int.min(0).optional,
)

StoryOutputSchemaFields = StorySchemaFields.copy()
StoryOutputSchemaFields.update(
    dt_published=T.datetime.optional,
    dt_updated=T.datetime.optional,
)

FeedSchemaFields = dict(
    url=T.url,
    use_proxy=T.bool.default(False),
    title=T.str,
    content_length=T.int.optional,
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
    response_status=T.int.optional,
    checksum_data=T.bytes.maxlen(4096).optional,
    warnings=T.str.optional,
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

FeedInfoSchemaFieldNames = [
    'response_status',
    'warnings',
]
FeedInfoSchemaFields = {
    k: FeedSchemaFields[k]
    for k in FeedInfoSchemaFieldNames
}
FeedInfoSchema = T.dict(
    **FeedInfoSchemaFields,
    status=T.str.default(FeedStatus.READY),
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
@profile_django_context
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
                reverse_url=reverse_url(url),
                title=feed_dict['title'],
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
            # only set UserFeed.title when import title not equal feed title
            title = None
            if feed_creation.title and feed_creation.title != feed.title:
                title = feed_creation.title
            user_feed = UserFeed(
                user_id=feed_creation.user_id,
                feed_id=feed.id,
                title=title,
                group=feed_creation.group,
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
@profile_django_context
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
        is_feed_url_changed = feed.url != feed_dict['url']
        if is_feed_url_changed:
            target_feed = Feed.get_first_by_url(feed_dict['url'])
            if target_feed:
                LOG.info(f'merge feed#{feed.id} url={feed.url} into '
                         f'feed#{target_feed.id} url={target_feed.url}')
                target_feed.merge(feed)
                return
        # only update dt_updated if has storys or feed fields updated
        is_feed_updated = bool(storys)
        for k, v in feed_dict.items():
            if k == 'dt_updated':
                continue
            if (v != '' and v is not None) or k in {'warnings'}:
                old_v = getattr(feed, k, None)
                if v != old_v:
                    is_feed_updated = True
                    setattr(feed, k, v)
        now = timezone.now()
        now_sub_30d = now - timezone.timedelta(days=30)
        if is_feed_updated:
            # set dt_updated to now, not trust rss date
            feed.dt_updated = now
        feed.dt_checked = feed.dt_synced = now
        feed.reverse_url = reverse_url(feed.url)
        feed.status = FeedStatus.READY
        feed.save()
    # save storys, bulk_save_by_feed has standalone transaction
    for s in storys:
        if not s['dt_updated']:
            s['dt_updated'] = now
        if not s['dt_published']:
            # set dt_published to now - 30d to avoid these storys
            # take over mushroom page, i.e. Story.query_recent_by_user
            s['dt_published'] = now_sub_30d
    modified_storys = STORY_SERVICE.bulk_save_by_feed(feed.id, storys, is_refresh=is_refresh)
    LOG.info(
        'feed#%s save storys total=%s num_modified=%s',
        feed.id, len(storys), len(modified_storys)
    )
    feed = Feed.get_by_pk(feed_id)
    is_freezed = feed.freeze_level is None or feed.freeze_level > 1
    if modified_storys and is_freezed:
        Feed.unfreeze_by_id(feed_id)
    need_fetch_story = _is_feed_need_fetch_storys(feed, modified_storys)
    for story in modified_storys:
        if not story.link:
            continue
        if need_fetch_story and (not _is_fulltext_story(story)):
            text = processor.story_html_to_text(story.content)
            num_sub_sentences = len(split_sentences(text))
            ctx.tell('worker_rss.fetch_story', dict(
                url=story.link,
                use_proxy=feed.use_proxy,
                feed_id=story.feed_id,
                offset=story.offset,
                num_sub_sentences=num_sub_sentences,
            ))


def _is_fulltext_story(story):
    if story.iframe_url or story.audio_url or story.image_url:
        return True
    return is_fulltext_content(StoryContentInfo(story.content))


T_ACCEPT = T.enum(','.join(FulltextAcceptStrategy.__members__))
_TIMEOUT_ERRORS = (socket.timeout, TimeoutError, requests.exceptions.Timeout)


@actor('harbor_rss.sync_story_fulltext')
def do_sync_story_fulltext(
    ctx: ActorContext,
    feed_id: T.int,
    offset: T.int,
) -> T.dict(
    feed_id=T.int,
    offset=T.int.min(0),
    use_proxy=T.bool,
    url=T.url,
    response_status=T.int,
    accept=T_ACCEPT,
):
    with log_django_context_metric('harbor_rss.sync_story_fulltext:read'):
        feed = Feed.get_by_pk(feed_id, detail='+use_proxy')
        story = STORY_SERVICE.get_by_offset(feed_id, offset, detail=True)
    assert story, f'story#{feed_id},{offset} not found'
    story_content_info = StoryContentInfo(story.content)
    num_sub_sentences = len(split_sentences(story_content_info.text))
    ret = dict(
        feed_id=feed_id,
        offset=offset,
        url=story.link,
        use_proxy=feed.use_proxy,
        accept=FulltextAcceptStrategy.REJECT.value,
    )
    try:
        result = ctx.ask('worker_rss.fetch_story', dict(
            url=story.link,
            use_proxy=feed.use_proxy,
            feed_id=feed_id,
            offset=offset,
            num_sub_sentences=num_sub_sentences,
        ))
    except _TIMEOUT_ERRORS as ex:
        LOG.error(f'Ask worker_rss.fetch_story timeout: {ex}')
        ret.update(response_status=FeedResponseStatus.CONNECTION_TIMEOUT)
        return ret
    else:
        ret.update(
            response_status=result['response_status'],
            use_proxy=result['use_proxy'],
        )
        if not result['content']:
            return ret
    with log_django_context_metric('harbor_rss.sync_story_fulltext:write'):
        accept = _update_story(
            story=story,
            story_content_info=story_content_info,
            content=result['content'],
            summary=None,  # not need update summary
            url=result['url'],
            sentence_count=result['sentence_count'],
        )
        ret.update(accept=accept.value)
    return ret


@actor('harbor_rss.update_feed_info')
@django_context
def do_update_feed_info(
    ctx: ActorContext,
    feed_id: T.int,
    feed: FeedInfoSchema,
):
    with transaction.atomic():
        feed_dict = feed
        feed = Feed.get_by_pk(feed_id)
        for k, v in feed_dict.items():
            setattr(feed, k, v)
        feed.dt_updated = timezone.now()
        feed.save()


def is_rssant_changelog(url: str):
    """
    >>> is_rssant_changelog('http://localhost:6789/changelog?version=1.0.0')
    True
    >>> is_rssant_changelog('https://rss.anyant.com/changelog.atom')
    True
    >>> is_rssant_changelog('https://rss.qa.anyant.com/changelog.atom')
    True
    >>> is_rssant_changelog('https://www.anyant.com/')
    False
    """
    is_rssant = 'rss' in url and 'anyant.com' in url
    is_local_rssant = url.startswith(CONFIG.root_url)
    return (is_rssant or is_local_rssant) and 'changelog' in url


def _is_feed_need_fetch_storys(feed, modified_storys):
    checkers = [
        processor.is_v2ex, processor.is_hacknews,
        processor.is_github, processor.is_pypi,
        is_rssant_changelog,
    ]
    for check in checkers:
        if check(feed.url):
            return False
    # eg: news, forum, bbs, daily reports
    if feed.dryness is not None and feed.dryness < 500:
        return False
    return True


@actor('harbor_rss.update_story')
@django_context
@profile_django_context
def do_update_story(
    ctx: ActorContext,
    feed_id: T.int,
    offset: T.int,
    content: T.str,
    summary: T.str,
    has_mathjax: T.bool.optional,
    url: T.url,
    response_status: T.int.optional,
    sentence_count: T.int.min(0).optional
):
    story = STORY_SERVICE.get_by_offset(feed_id, offset, detail=True)
    if not story:
        LOG.error('story#%s,%s not found', feed_id, offset)
        return
    _update_story(
        story=story,
        story_content_info=StoryContentInfo(story.content),
        content=content,
        summary=summary,
        url=url,
        has_mathjax=has_mathjax,
        sentence_count=sentence_count,
    )


def _update_story(
    story: CommonStory,
    story_content_info: StoryContentInfo,
    content: str,
    summary: str,
    url: str,
    has_mathjax: bool = None,
    sentence_count: int = None
) -> FulltextAcceptStrategy:
    new_info = StoryContentInfo(content)
    accept = decide_accept_fulltext(new_info, story_content_info)
    if accept == FulltextAcceptStrategy.REJECT:
        msg = 'fetched story#%s,%s url=%r is not fulltext of feed story content'
        LOG.info(msg, story.feed_id, story.offset, url)
        return accept
    if accept == FulltextAcceptStrategy.APPEND:
        content = (story.content or '') + '\n<hr/>\n' + (content or '')
    data = dict(
        link=url,
        content=content,
        summary=summary,
        has_mathjax=has_mathjax,
        sentence_count=sentence_count,
    )
    STORY_SERVICE.update_story(story.feed_id, story.offset, data)
    return accept


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
            etag=feed['etag'],
            last_modified=feed['last_modified'],
            use_proxy=feed['use_proxy'],
            checksum_data=feed['checksum_data'],
        ), expire_at=expire_at)


@actor('harbor_rss.clean_feed_creation')
@django_context
def do_clean_feed_creation(ctx: ActorContext):
    # 删除所有入库时间超过24小时的订阅创建信息
    num_deleted = FeedCreation.delete_by_status(survival_seconds=24 * 60 * 60)
    LOG.info('delete {} old feed creations'.format(num_deleted))
    # 重试 status=UPDATING 超过4小时的订阅
    feed_creation_id_urls = FeedCreation.query_id_urls_by_status(
        FeedStatus.UPDATING, survival_seconds=4 * 60 * 60)
    num_retry_updating = len(feed_creation_id_urls)
    LOG.info('retry {} status=UPDATING feed creations'.format(num_retry_updating))
    _retry_feed_creations(ctx, feed_creation_id_urls)
    # 重试 status=PENDING 超过4小时的订阅
    feed_creation_id_urls = FeedCreation.query_id_urls_by_status(
        FeedStatus.PENDING, survival_seconds=4 * 60 * 60)
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
    feeds = Feed.take_retention_feeds(retention=retention, limit=50)
    LOG.info('found {} feeds need clean by retention'.format(len(feeds)))
    for feed in feeds:
        feed_id = feed['feed_id']
        url = feed['url']
        n = STORY_SERVICE.delete_by_retention(feed_id, retention=retention)
        LOG.info(f'deleted {n} storys of feed#{feed_id} {url} by retention')


@actor('harbor_rss.clean_feedurlmap_by_retention')
@django_context
def do_clean_feedurlmap_by_retention(ctx: ActorContext):
    num_rows = FeedUrlMap.delete_by_retention()
    LOG.info('delete {} outdated feedurlmap'.format(num_rows))


@actor('harbor_rss.feed_refresh_freeze_level')
@django_context
def do_feed_refresh_freeze_level(ctx: ActorContext):
    begin_time = time.time()
    Feed.refresh_freeze_level()
    cost = time.time() - begin_time
    LOG.info('feed_refresh_freeze_level cost {:.1f}ms'.format(cost * 1000))


def _feed_merge_duplicate(found: list):
    for feed_ids in found:
        primary_id, *duplicates = feed_ids
        with transaction.atomic():
            primary = Feed.get_by_pk(primary_id)
            primary_info = f'#{primary.id} url={primary.url!r}'
            for feed_id in duplicates:
                other = Feed.get_by_pk(feed_id)
                other_info = f'#{other.id} url={other.url!r}'
                LOG.info('merge duplicate feed %s into %s', other_info, primary_info)
                FeedUrlMap(source=other.url, target=primary.url).save()
                primary.merge(other)


@actor('harbor_rss.feed_detect_and_merge_duplicate')
@django_context
def do_feed_detect_and_merge_duplicate(ctx: ActorContext):
    begin_time = time.time()
    checkpoint = None
    while True:
        found, checkpoint = Feed.find_duplicate_feeds(checkpoint=checkpoint)
        _feed_merge_duplicate(found)
        if not checkpoint:
            break
    cost = time.time() - begin_time
    LOG.info('feed_detect_and_merge_duplicate cost {:.1f}ms'.format(cost * 1000))
