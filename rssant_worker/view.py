import logging

from django_rest_validr import RestRouter, T
from rssant_api.views.common import AllowServiceClient

from .worker_service import SCHEMA_FETCH_STORY_RESULT, WORKER_SERVICE

LOG = logging.getLogger(__name__)

WorkerView = RestRouter(permission_classes=[AllowServiceClient])


@WorkerView.post('worker_rss.find_feed')
def do_find_feed(
    request,
    feed_creation_id: T.int,
    url: T.url,
):
    return WORKER_SERVICE.find_feed(
        feed_creation_id=feed_creation_id,
        url=url,
    )


@WorkerView.post('worker_rss.sync_feed')
def do_sync_feed(
    request,
    feed_id: T.int,
    url: T.url,
    use_proxy: T.bool.default(False),
    checksum_data_base64: T.str.maxlen(8192).optional,
    content_hash_base64: T.str.optional,
    etag: T.str.optional,
    last_modified: T.str.optional,
    is_refresh: T.bool.default(False),
):
    return WORKER_SERVICE.sync_feed(
        feed_id=feed_id,
        url=url,
        use_proxy=use_proxy,
        checksum_data_base64=checksum_data_base64,
        content_hash_base64=content_hash_base64,
        etag=etag,
        last_modified=last_modified,
        is_refresh=is_refresh,
    )


@WorkerView.post('worker_rss.fetch_story')
def do_fetch_story(
    request,
    feed_id: T.int,
    offset: T.int.min(0),
    url: T.url,
    use_proxy: T.bool.default(False),
    num_sub_sentences: T.int.optional,
) -> SCHEMA_FETCH_STORY_RESULT:
    return WORKER_SERVICE.fetch_story(
        feed_id=feed_id,
        offset=offset,
        url=url,
        use_proxy=use_proxy,
        num_sub_sentences=num_sub_sentences,
    )
