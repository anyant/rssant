import logging
import random
from urllib.parse import unquote

from validr import T, Invalid
from attrdict import AttrDict
from django.utils import timezone

from actorlib import actor, ActorContext

from rssant_feedlib import AsyncFeedReader, FeedResponseStatus
from rssant_feedlib import (
    FeedFinder, FeedReader,
    FeedParser, RawFeedParser, FeedChecksum,
    RawFeedResult, FeedResponse, FeedParserError,
)
from rssant_feedlib.processor import (
    story_readability, story_html_to_text, story_html_clean,
    process_story_links, get_html_redirect_url,
)
from rssant_feedlib.fulltext import is_fulltext_content, StoryContentInfo, split_sentences
from rssant.helper.content_hash import compute_hash_base64
from rssant_api.models import FeedStatus
from rssant_api.helper import shorten
from rssant_config import CONFIG
from rssant_common.rss import (
    validate_feed as _validate_feed,
    validate_story as _validate_story,
    get_story_of_feed_entry,
)
from rssant_common import _proxy_helper
from rssant_common.dns_service import DNS_SERVICE


LOG = logging.getLogger(__name__)


_MAX_STORY_HTML_LENGTH = 5 * 1000 * 1024
_MAX_STORY_CONTENT_LENGTH = 1000 * 1024
_MAX_STORY_SUMMARY_LENGTH = 300


def validate_feed(feed):
    feed_info = feed.get('url') or feed.get('link') or feed.get('title')
    try:
        feed_data = _validate_feed(feed)
    except Invalid as ex:
        ex.args = (f'{ex.args[0]}, feed={feed_info}', *ex.args[1:])
        raise
    storys = []
    for story in feed_data['storys']:
        try:
            story = _validate_story(story)
        except Invalid as ex:
            story_info = story.get('link') or story.get('title') or story.get('link')
            LOG.error('%s, feed=%s, story=%s', ex, feed_info, story_info)
        else:
            storys.append(story)
    feed_data['storys'] = storys
    return feed_data


@actor('worker_rss.find_feed')
def do_find_feed(
    ctx: ActorContext,
    feed_creation_id: T.int,
    url: T.url,
):
    # immediately send message to update status
    ctx.ask('harbor_rss.update_feed_creation_status', dict(
        feed_creation_id=feed_creation_id,
        status=FeedStatus.UPDATING,
    ))

    messages = []

    def message_handler(msg):
        LOG.info(msg)
        messages.append(msg)

    options = _proxy_helper.get_proxy_options()
    options.update(message_handler=message_handler)
    options.update(request_timeout=CONFIG.feed_reader_request_timeout)
    options.update(dns_service=DNS_SERVICE)
    with FeedFinder(url, **options) as finder:
        found = finder.find()
    try:
        feed = _parse_found(found) if found else None
    except (Invalid, FeedParserError) as ex:
        LOG.error('invalid feed url=%r: %s', unquote(url), ex, exc_info=ex)
        message_handler(f'invalid feed: {ex}')
        feed = None
    ctx.tell('harbor_rss.save_feed_creation_result', dict(
        feed_creation_id=feed_creation_id,
        messages=messages,
        feed=feed,
    ))


@actor('worker_rss.sync_feed')
def do_sync_feed(
    ctx: ActorContext,
    feed_id: T.int,
    url: T.url,
    use_proxy: T.bool.default(False),
    checksum_data: T.bytes.maxlen(4096).optional,
    content_hash_base64: T.str.optional,
    etag: T.str.optional,
    last_modified: T.str.optional,
    is_refresh: T.bool.default(False),
):
    params = {}
    if not is_refresh:
        params = dict(etag=etag, last_modified=last_modified)
    options = _proxy_helper.get_proxy_options()
    options.update(request_timeout=CONFIG.feed_reader_request_timeout)
    if DNS_SERVICE.is_resolved_url(url):
        use_proxy = False
    switch_prob = 0.25  # the prob of switch from use proxy to not use proxy
    with FeedReader(**options) as reader:
        use_proxy = reader.has_proxy and use_proxy
        if use_proxy and random.random() < switch_prob:
            use_proxy = False
        response = reader.read(url, **params, use_proxy=use_proxy)
        LOG.info(f'read feed#{feed_id} url={unquote(url)} status={response.status}')
        need_proxy = FeedResponseStatus.is_need_proxy(response.status)
        if (not use_proxy) and reader.has_proxy and need_proxy:
            LOG.info(f'try use proxy read feed#{feed_id} url={unquote(url)}')
            proxy_response = reader.read(url, **params, use_proxy=True)
            LOG.info(f'proxy read feed#{feed_id} url={unquote(url)} status={proxy_response.status}')
            if proxy_response.ok:
                response = proxy_response
    if (not response.ok) or (not response.content):
        status = FeedStatus.READY if response.status == 304 else FeedStatus.ERROR
        _update_feed_info(ctx, feed_id, status=status, response=response)
        return
    new_hash = compute_hash_base64(response.content)
    if (not is_refresh) and (new_hash == content_hash_base64):
        LOG.info(f'feed#{feed_id} url={unquote(url)} not modified by compare content hash!')
        _update_feed_info(ctx, feed_id, response=response)
        return
    LOG.info(f'parse feed#{feed_id} url={unquote(url)}')
    try:
        raw_result = RawFeedParser().parse(response)
    except FeedParserError as ex:
        LOG.warning('failed parse feed#%s url=%r: %s', feed_id, unquote(url), ex)
        _update_feed_info(
            ctx, feed_id, status=FeedStatus.ERROR,
            response=response, warnings=str(ex))
        return
    if raw_result.warnings:
        warnings = '; '.join(raw_result.warnings)
        LOG.warning('warning parse feed#%s url=%r: %s', feed_id, unquote(url), warnings)
    try:
        feed = _parse_found(
            (response, raw_result),
            checksum_data=checksum_data, is_refresh=is_refresh)
    except (Invalid, FeedParserError) as ex:
        LOG.error('invalid feed#%s url=%r: %s', feed_id, unquote(url), ex, exc_info=ex)
        _update_feed_info(
            ctx, feed_id, status=FeedStatus.ERROR,
            response=response, warnings=str(ex))
        return
    ctx.tell('harbor_rss.update_feed', dict(feed_id=feed_id, feed=feed, is_refresh=is_refresh))


def _update_feed_info(ctx, feed_id, response: FeedResponse, status: str = None, warnings: str = None):
    ctx.tell('harbor_rss.update_feed_info', dict(
        feed_id=feed_id,
        feed=dict(
            status=status,
            response_status=response.status,
            warnings=warnings,
        )
    ))


async def _fetch_story(reader: AsyncFeedReader, feed_id, offset, url, use_proxy) -> tuple:
    for i in range(2):
        response = await reader.read(url, use_proxy=use_proxy)
        if response and response.url:
            url = str(response.url)
        LOG.info(
            f'fetch story#{feed_id},{offset} url={unquote(url)} status={response.status} finished')
        if not (response and response.ok and response.content):
            return url, None, response
        try:
            content = response.content.decode(response.encoding)
        except UnicodeDecodeError as ex:
            LOG.warning('fetch story unicode decode error=%s url=%r', ex, url)
            content = response.content.decode(response.encoding, errors='ignore')
        html_redirect = get_html_redirect_url(content)
        if (not html_redirect) or html_redirect == url:
            return url, content, response
        LOG.info('story#%s,%s resolve html redirect to %r', feed_id, offset, html_redirect)
        url = html_redirect
    return url, content, response


SCHEMA_FETCH_STORY_RESULT = T.dict(
    feed_id=T.int,
    offset=T.int.min(0),
    url=T.url,
    response_status=T.int.optional,
    use_proxy=T.bool.optional,
    content=T.str.maxlen(_MAX_STORY_HTML_LENGTH).optional,
    summary=T.str.optional,
    sentence_count=T.int.optional,
)


@actor('worker_rss.fetch_story')
async def do_fetch_story(
    ctx: ActorContext,
    feed_id: T.int,
    offset: T.int.min(0),
    url: T.url,
    use_proxy: T.bool.default(False),
    num_sub_sentences: T.int.optional,
) -> SCHEMA_FETCH_STORY_RESULT:
    LOG.info(f'fetch story#{feed_id},{offset} url={unquote(url)} begin')
    options = _proxy_helper.get_proxy_options()
    if DNS_SERVICE.is_resolved_url(url):
        use_proxy = False
    # make timeout less than actor default 30s to avoid ask timeout
    options.update(request_timeout=25)
    async with AsyncFeedReader(**options) as reader:
        use_proxy = use_proxy and reader.has_proxy
        url, content, response = await _fetch_story(reader, feed_id, offset, url, use_proxy=use_proxy)
    DEFAULT_RESULT = dict(
        feed_id=feed_id, offset=offset, url=url,
        response_status=response.status, use_proxy=response.use_proxy)
    if not content:
        return DEFAULT_RESULT
    if len(content) >= _MAX_STORY_HTML_LENGTH:
        content = story_html_clean(content)
        if len(content) >= _MAX_STORY_HTML_LENGTH:
            msg = 'too large story#%s,%s size=%s url=%r'
            LOG.warning(msg, feed_id, offset, len(content), url)
            content = story_html_to_text(content)[:_MAX_STORY_HTML_LENGTH]
    msg_func = ctx.ask if ctx.message.is_ask else ctx.hope
    result = await msg_func('worker_rss.process_story_webpage', dict(
        feed_id=feed_id,
        offset=offset,
        url=url,
        text=content,
        num_sub_sentences=num_sub_sentences,
    ))
    if not ctx.message.is_ask:
        return DEFAULT_RESULT
    result.update(DEFAULT_RESULT)
    return result


@actor('worker_rss.process_story_webpage')
def do_process_story_webpage(
    ctx: ActorContext,
    feed_id: T.int,
    offset: T.int,
    url: T.url,
    text: T.str.maxlen(_MAX_STORY_HTML_LENGTH),
    num_sub_sentences: T.int.optional,
) -> SCHEMA_FETCH_STORY_RESULT:
    # https://github.com/dragnet-org/dragnet
    # https://github.com/misja/python-boilerpipe
    # https://github.com/dalab/web2text
    # https://github.com/grangier/python-goose
    # https://github.com/buriy/python-readability
    # https://github.com/codelucas/newspaper
    DEFAULT_RESULT = dict(feed_id=feed_id, offset=offset, url=url)
    text = text.strip()
    if not text:
        return DEFAULT_RESULT
    text = story_html_clean(text)
    content = story_readability(text)
    content = process_story_links(content, url)
    content_info = StoryContentInfo(content)
    text_content = shorten(content_info.text, width=_MAX_STORY_CONTENT_LENGTH)
    num_sentences = len(split_sentences(text_content))
    if len(content) > _MAX_STORY_CONTENT_LENGTH:
        msg = 'too large story#%s,%s size=%s url=%r, will only save plain text'
        LOG.warning(msg, feed_id, offset, len(content), url)
        content = text_content
    # 如果取回的内容比RSS内容更短，就不是正确的全文
    if num_sub_sentences is not None:
        if not is_fulltext_content(content_info):
            if num_sentences <= num_sub_sentences:
                msg = 'fetched story#%s,%s url=%s num_sentences=%s less than num_sub_sentences=%s'
                LOG.info(msg, feed_id, offset, url, num_sentences, num_sub_sentences)
                return DEFAULT_RESULT
    summary = shorten(text_content, width=_MAX_STORY_SUMMARY_LENGTH)
    if not summary:
        return DEFAULT_RESULT
    result = dict(
        **DEFAULT_RESULT,
        content=content,
        summary=summary,
        sentence_count=num_sentences,
    )
    if not ctx.message.is_ask:
        ctx.hope('harbor_rss.update_story', result)
    return result


def _parse_found(found, checksum_data=None, is_refresh=False):
    response: FeedResponse
    raw_result: RawFeedResult
    response, raw_result = found
    feed = AttrDict()

    # feed response
    feed.use_proxy = response.use_proxy
    feed.url = response.url
    feed.content_length = len(response.content)
    feed.content_hash_base64 = compute_hash_base64(response.content)
    feed.etag = response.etag
    feed.last_modified = response.last_modified
    feed.encoding = response.encoding
    feed.response_status = response.status
    del found, response  # release memory in advance

    # parse feed and storys
    checksum = None
    if checksum_data and (not is_refresh):
        checksum = FeedChecksum.load(checksum_data)
    result = FeedParser(checksum=checksum).parse(raw_result)
    checksum_data = result.checksum.dump(limit=300)
    num_raw_storys = len(raw_result.storys)
    warnings = None
    if raw_result.warnings:
        warnings = '; '.join(raw_result.warnings)
    del raw_result  # release memory in advance
    msg = "feed url=%r storys=%s changed_storys=%s"
    LOG.info(msg, feed.url, num_raw_storys, len(result.storys))

    feed.title = result.feed['title']
    feed.link = result.feed['home_url']
    feed.author = result.feed['author_name']
    feed.icon = result.feed['icon_url']
    feed.description = result.feed['description']
    feed.dt_updated = result.feed['dt_updated']
    feed.version = result.feed['version']
    feed.storys = _get_storys(result.storys)
    feed.checksum_data = checksum_data
    feed.warnings = warnings
    del result  # release memory in advance

    return validate_feed(feed)


def _get_storys(entries: list):
    storys = []
    now = timezone.now()
    for data in entries:
        story = get_story_of_feed_entry(data, now=now)
        storys.append(story)
    return storys
