import logging
import time

from django.db import transaction
from django.utils import timezone
from validr import T

from rssant_api.helper import reverse_url
from rssant_api.models import (
    STORY_SERVICE,
    CommonStory,
    Feed,
    FeedCreation,
    FeedStatus,
    FeedUrlMap,
    UserFeed,
)
from rssant_common.base64 import UrlsafeBase64
from rssant_config import CONFIG
from rssant_feedlib import processor
from rssant_feedlib.do_not_fetch_fulltext import is_not_fetch_fulltext
from rssant_feedlib.fulltext import (
    FulltextAcceptStrategy,
    StoryContentInfo,
    decide_accept_fulltext,
    is_fulltext_content,
    split_sentences,
)

from .schema import FeedInfoSchema, FeedSchema, validate_feed_output

LOG = logging.getLogger(__name__)


def _is_feed_need_fetch_storys(feed, modified_storys):
    if is_not_fetch_fulltext(feed.url):
        return False
    # eg: news, forum, bbs, daily reports
    if feed.dryness is not None and feed.dryness < 500:
        return False
    return True


def _is_fulltext_story(story):
    if story.iframe_url or story.audio_url or story.image_url:
        return True
    return is_fulltext_content(StoryContentInfo(story.content))


def _update_story(
    story: CommonStory,
    story_content_info: StoryContentInfo,
    content: str,
    summary: str,
    url: str,
    has_mathjax: bool = None,
    sentence_count: int = None,
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


class HarborService:

    def update_feed_creation_status(
        self,
        feed_creation_id: int,
        status: str,
    ):
        with transaction.atomic():
            FeedCreation.objects.filter(pk=feed_creation_id).update(status=status)

    def save_feed_creation_result(
        self,
        feed_creation_id: int,
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
                    url=url,
                    status=FeedStatus.READY,
                    reverse_url=reverse_url(url),
                    title=feed_dict['title'],
                    dt_updated=now,
                    dt_checked=now,
                    dt_synced=now,
                )
                feed.save()
            feed_creation.status = FeedStatus.READY
            feed_creation.feed_id = feed.id
            feed_creation.save()
            user_feed = UserFeed.objects.filter(
                user_id=feed_creation.user_id, feed_id=feed.id
            ).first()
            if user_feed:
                LOG.info(
                    'UserFeed#{} user_id={} feed_id={} already exists'.format(
                        user_feed.id, feed_creation.user_id, feed.id
                    )
                )
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
        self.update_feed(
            feed_id=feed.id,
            feed=validate_feed_output(feed_dict),
        )

    def _convert_checksum_data(self, feed_dict: dict):
        feed_dict['checksum_data'] = UrlsafeBase64.decode(
            feed_dict.pop('checksum_data_base64', None)
        )

    def update_feed(
        self,
        feed_id: int,
        feed: FeedSchema,
        is_refresh: bool = False,
    ):
        feed_dict = feed
        self._convert_checksum_data(feed_dict)
        with transaction.atomic():
            storys = feed_dict.pop('storys')
            feed = Feed.get_by_pk(feed_id)
            is_feed_url_changed = feed.url != feed_dict['url']
            if is_feed_url_changed:
                target_feed = Feed.get_first_by_url(feed_dict['url'])
                # FIXME: feed merge 无法正确处理订阅重定向问题。
                # 对于这种情况，暂时保留旧的订阅，以后再彻底解决。
                # if target_feed:
                #     LOG.info(f'merge feed#{feed.id} url={feed.url} into '
                #              f'feed#{target_feed.id} url={target_feed.url}')
                #     target_feed.merge(feed)
                #     return
                if target_feed:
                    LOG.warning(
                        f'FIXME: redirect feed#{feed.id} url={feed.url!r} into '
                        f'feed#{target_feed.id} url={target_feed.url!r}'
                    )
                    feed_dict.pop('url')
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
        modified_storys = STORY_SERVICE.bulk_save_by_feed(
            feed.id, storys, is_refresh=is_refresh
        )
        LOG.info(
            'feed#%s save storys total=%s num_modified=%s',
            feed.id,
            len(storys),
            len(modified_storys),
        )
        feed = Feed.get_by_pk(feed_id)
        is_freezed = feed.freeze_level is None or feed.freeze_level > 1
        if modified_storys and is_freezed:
            Feed.unfreeze_by_id(feed_id)
        need_fetch_story = _is_feed_need_fetch_storys(feed, modified_storys)
        fetch_story_task_s = []
        for story in modified_storys:
            if not story.link:
                continue
            if need_fetch_story and (not _is_fulltext_story(story)):
                text = processor.story_html_to_text(story.content)
                num_sub_sentences = len(split_sentences(text))
                fetch_story_task_s.append(
                    dict(
                        url=story.link,
                        use_proxy=feed.use_proxy,
                        feed_id=story.feed_id,
                        offset=story.offset,
                        num_sub_sentences=num_sub_sentences,
                    )
                )
        return fetch_story_task_s

    def update_feed_info(
        self,
        feed_id: int,
        feed: FeedInfoSchema,
    ):
        feed_dict = feed
        self._convert_checksum_data(feed_dict)
        with transaction.atomic():
            feed = Feed.get_by_pk(feed_id)
            for k, v in feed_dict.items():
                setattr(feed, k, v)
            feed.dt_updated = timezone.now()
            feed.save()

    def update_story(
        self,
        feed_id: int,
        offset: int,
        content: str,
        summary: str,
        url: str,
        has_mathjax: bool = None,
        response_status: int = None,
        sentence_count: int = None,
    ):
        story = STORY_SERVICE.get_by_offset(feed_id, offset, detail=True)
        if not story:
            LOG.error('story#%s,%s not found', feed_id, offset)
            return
        accept = _update_story(
            story=story,
            story_content_info=StoryContentInfo(story.content),
            content=content,
            summary=summary,
            url=url,
            has_mathjax=has_mathjax,
            sentence_count=sentence_count,
        )
        return dict(accept=accept.value)

    def clean_feed_creation(self):
        # 删除所有入库时间超过24小时的订阅创建信息
        num_deleted = FeedCreation.delete_by_status(survival_seconds=24 * 60 * 60)
        LOG.info('delete {} old feed creations'.format(num_deleted))
        return dict(num_deleted=num_deleted)

    def clean_by_retention(self):
        retention = CONFIG.feed_story_retention
        feeds = Feed.take_retention_feeds(retention=retention, limit=50)
        LOG.info('found {} feeds need clean by retention'.format(len(feeds)))
        for feed in feeds:
            feed_id = feed['feed_id']
            url = feed['url']
            n = STORY_SERVICE.delete_by_retention(feed_id, retention=retention)
            LOG.info(f'deleted {n} storys of feed#{feed_id} {url} by retention')

    def clean_feedurlmap_by_retention(self):
        num_rows = FeedUrlMap.delete_by_retention()
        LOG.info('delete {} outdated feedurlmap'.format(num_rows))

    def feed_refresh_freeze_level(self):
        begin_time = time.time()
        Feed.refresh_freeze_level()
        cost = time.time() - begin_time
        LOG.info('feed_refresh_freeze_level cost {:.1f}ms'.format(cost * 1000))

    def feed_detect_and_merge_duplicate(self):
        begin_time = time.time()
        checkpoint = None
        while True:
            found, checkpoint = Feed.find_duplicate_feeds(checkpoint=checkpoint)
            _feed_merge_duplicate(found)
            if not checkpoint:
                break
        cost = time.time() - begin_time
        LOG.info('feed_detect_and_merge_duplicate cost {:.1f}ms'.format(cost * 1000))


HARBOR_SERVICE = HarborService()
