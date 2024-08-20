import logging

from django_rest_validr import RestRouter, T
from rssant_api.views.common import AllowServiceClient

from .django_service import django_clear_expired_sessions
from .harbor_service import HARBOR_SERVICE
from .schema import T_ACCEPT, FeedInfoSchema, FeedSchema

LOG = logging.getLogger(__name__)

HarborView = RestRouter(permission_classes=[AllowServiceClient])


@HarborView.post('harbor_django.django_clear_expired_sessions')
def do_django_clear_expired_sessions(request):
    django_clear_expired_sessions()


@HarborView.post('harbor_rss.update_feed_creation_status')
def do_update_feed_creation_status(
    request,
    feed_creation_id: T.int,
    status: T.str,
):
    HARBOR_SERVICE.update_feed_creation_status(
        feed_creation_id=feed_creation_id,
        status=status,
    )


@HarborView.post('harbor_rss.save_feed_creation_result')
def do_save_feed_creation_result(
    request,
    feed_creation_id: T.int,
    messages: T.list(T.str),
    feed: FeedSchema.optional,
) -> T.any:
    return HARBOR_SERVICE.save_feed_creation_result(
        feed_creation_id=feed_creation_id,
        messages=messages,
        feed=feed,
    )


@HarborView.post('harbor_rss.update_feed')
def do_update_feed(
    request,
    feed_id: T.int,
    feed: FeedSchema,
    is_refresh: T.bool.default(False),
) -> T.any:
    return HARBOR_SERVICE.update_feed(
        feed_id=feed_id,
        feed=feed,
        is_refresh=is_refresh,
    )


@HarborView.post('harbor_rss.sync_story_fulltext')
def do_sync_story_fulltext(
    request,
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
    pass


@HarborView.post('harbor_rss.update_feed_info')
def do_update_feed_info(
    request,
    feed_id: T.int,
    feed: FeedInfoSchema,
):
    HARBOR_SERVICE.update_feed_info(
        feed_id=feed_id,
        feed=feed,
    )


@HarborView.post('harbor_rss.update_story')
def do_update_story(
    request,
    feed_id: T.int,
    offset: T.int,
    content: T.str,
    summary: T.str,
    has_mathjax: T.bool.optional,
    url: T.url,
    response_status: T.int.optional,
    sentence_count: T.int.min(0).optional,
) -> T.any:
    return HARBOR_SERVICE.update_story(
        feed_id=feed_id,
        offset=offset,
        content=content,
        summary=summary,
        has_mathjax=has_mathjax,
        url=url,
        response_status=response_status,
        sentence_count=sentence_count,
    )


@HarborView.post('harbor_rss.clean_feed_creation')
def do_clean_feed_creation(request):
    HARBOR_SERVICE.clean_feed_creation()


@HarborView.post('harbor_rss.clean_by_retention')
def do_clean_by_retention(request):
    HARBOR_SERVICE.clean_by_retention()


@HarborView.post('harbor_rss.clean_feedurlmap_by_retention')
def do_clean_feedurlmap_by_retention(request):
    HARBOR_SERVICE.clean_feedurlmap_by_retention()


@HarborView.post('harbor_rss.feed_refresh_freeze_level')
def do_feed_refresh_freeze_level(request):
    HARBOR_SERVICE.feed_refresh_freeze_level()


@HarborView.post('harbor_rss.feed_detect_and_merge_duplicate')
def do_feed_detect_and_merge_duplicate(request):
    HARBOR_SERVICE.feed_detect_and_merge_duplicate()
