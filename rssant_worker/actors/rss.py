import logging
import asyncio
import time
import random
from urllib.parse import unquote
import concurrent.futures

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
from rssant_feedlib.blacklist import compile_url_blacklist
from rssant_feedlib.fulltext import is_fulltext_content, split_sentences

from rssant.helper.content_hash import compute_hash_base64
from rssant_api.models import FeedStatus
from rssant_api.helper import shorten
from rssant_common import _proxy_helper
from rssant_common.validator import compiler
from rssant_common.dns_service import DNS_SERVICE


LOG = logging.getLogger(__name__)


_MAX_STORY_HTML_LENGTH = 5 * 1000 * 1024
_MAX_STORY_CONTENT_LENGTH = 1000 * 1024
_MAX_STORY_SUMMARY_LENGTH = 300


REFERER_DENY_LIST = """
qpic.cn
qlogo.cn
qq.com
"""
is_referer_deny_url = compile_url_blacklist(REFERER_DENY_LIST)


StorySchema = T.dict(
    unique_id=T.str,
    title=T.str,
    content_hash_base64=T.str,
    author=T.str.optional,
    link=T.url.optional,
    image_url=T.url.optional,
    iframe_url=T.url.optional,
    audio_url=T.url.optional,
    has_mathjax=T.bool.optional,
    dt_published=T.datetime.optional,
    dt_updated=T.datetime.optional,
    summary=T.str.optional,
    content=T.str.optional,
    sentence_count=T.int.min(0).optional,
)

FeedSchema = T.dict(
    url=T.url,
    use_proxy=T.bool.default(False),
    title=T.str,
    content_length=T.int,
    content_hash_base64=T.str,
    link=T.url.optional,
    author=T.str.optional,
    icon=T.str.optional,
    description=T.str.optional,
    version=T.str.optional,
    dt_updated=T.datetime.optional,
    encoding=T.str.optional,
    etag=T.str.optional,
    last_modified=T.str.optional,
    response_status=T.int.optional,
    checksum_data=T.bytes.maxlen(4096).optional,
    warnings=T.str.optional,
    storys=T.list,
)

_validate_feed = compiler.compile(FeedSchema)
_validate_story = compiler.compile(StorySchema)


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

    options = dict(message_handler=message_handler, **_proxy_helper.get_proxy_options())
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


async def _fetch_story(reader, feed_id, offset, url, use_proxy):
    for i in range(2):
        response = await reader.read(url, use_proxy=use_proxy)
        if response and response.url:
            url = str(response.url)
        LOG.info(
            f'fetch story#{feed_id},{offset} url={unquote(url)} status={response.status} finished')
        if not (response and response.ok and response.content):
            return None
        try:
            content = response.content.decode(response.encoding)
        except UnicodeDecodeError as ex:
            LOG.warning('fetch story unicode decode error=%s url=%r', ex, url)
            content = response.content.decode(response.encoding, errors='ignore')
        html_redirect = get_html_redirect_url(content)
        if (not html_redirect) or html_redirect == url:
            return url, content
        LOG.info('story#%s,%s resolve html redirect to %r', feed_id, offset, html_redirect)
        url = html_redirect
    return url, content


@actor('worker_rss.fetch_story')
async def do_fetch_story(
    ctx: ActorContext,
    feed_id: T.int,
    offset: T.int,
    url: T.url,
    use_proxy: T.bool.default(False),
    num_sub_sentences: T.int.optional,
):
    LOG.info(f'fetch story#{feed_id},{offset} url={unquote(url)} begin')
    options = _proxy_helper.get_proxy_options()
    if DNS_SERVICE.is_resolved_url(url):
        use_proxy = False
    async with AsyncFeedReader(**options) as reader:
        use_proxy = use_proxy and reader.has_proxy
        url_content = await _fetch_story(reader, feed_id, offset, url, use_proxy=use_proxy)
    if not url_content:
        return
    url, content = url_content
    if len(content) >= _MAX_STORY_HTML_LENGTH:
        content = story_html_clean(content)
        if len(content) >= _MAX_STORY_HTML_LENGTH:
            msg = 'too large story#%s,%s size=%s url=%r'
            LOG.warning(msg, feed_id, offset, len(content), url)
            content = story_html_to_text(content)[:_MAX_STORY_HTML_LENGTH]
    await ctx.hope('worker_rss.process_story_webpage', dict(
        feed_id=feed_id,
        offset=offset,
        url=url,
        text=content,
        num_sub_sentences=num_sub_sentences,
    ))


@actor('worker_rss.process_story_webpage')
def do_process_story_webpage(
    ctx: ActorContext,
    feed_id: T.int,
    offset: T.int,
    url: T.url,
    text: T.str.maxlen(_MAX_STORY_HTML_LENGTH),
    num_sub_sentences: T.int.optional,
):
    # https://github.com/dragnet-org/dragnet
    # https://github.com/misja/python-boilerpipe
    # https://github.com/dalab/web2text
    # https://github.com/grangier/python-goose
    # https://github.com/buriy/python-readability
    # https://github.com/codelucas/newspaper
    text = text.strip()
    if not text:
        return
    text = story_html_clean(text)
    content = story_readability(text)
    content = process_story_links(content, url)
    text_content = shorten(story_html_to_text(content), width=_MAX_STORY_CONTENT_LENGTH)
    num_sentences = len(split_sentences(text_content))
    if len(content) > _MAX_STORY_CONTENT_LENGTH:
        msg = 'too large story#%s,%s size=%s url=%r, will only save plain text'
        LOG.warning(msg, feed_id, offset, len(content), url)
        content = text_content
    # 如果取回的内容比RSS内容更短，就不是正确的全文
    if num_sub_sentences is not None:
        if not is_fulltext_content(content):
            if num_sentences <= num_sub_sentences:
                msg = 'fetched story#%s,%s url=%s num_sentences=%s less than num_sub_sentences=%s'
                LOG.info(msg, feed_id, offset, url, num_sentences, num_sub_sentences)
                return
    summary = shorten(text_content, width=_MAX_STORY_SUMMARY_LENGTH)
    if not summary:
        return
    ctx.hope('harbor_rss.update_story', dict(
        feed_id=feed_id,
        offset=offset,
        content=content,
        summary=summary,
        url=url,
        sentence_count=num_sentences,
    ))


@actor('worker_rss.detect_story_images')
async def do_detect_story_images(
    ctx: ActorContext,
    feed_id: T.int,
    offset: T.int,
    story_url: T.url,
    image_urls: T.list(T.url).unique,
):
    LOG.info(f'detect story images story={feed_id},{offset} num_images={len(image_urls)} begin')
    options = dict(
        allow_non_webpage=True,
        dns_service=DNS_SERVICE,
    )
    async with AsyncFeedReader(**options) as reader:
        async def _read(url):
            if is_referer_deny_url(url):
                return url, FeedResponseStatus.REFERER_DENY.value
            response = await reader.read(
                url,
                referer="https://rss.anyant.com/",
                ignore_content=True
            )
            return url, response.status
        futs = []
        for url in image_urls:
            futs.append(asyncio.ensure_future(_read(url)))
        t_begin = time.time()
        try:
            results = await asyncio.gather(*futs)
        except (TimeoutError, concurrent.futures.TimeoutError):
            results = [fut.result() for fut in futs if fut.done()]
        cost_ms = (time.time() - t_begin) * 1000
    num_ok = num_error = 0
    images = []
    for url, status in results:
        if status == 200:
            num_ok += 1
        else:
            num_error += 1
        images.append(dict(url=url, status=status))
    LOG.info(f'detect story images story={feed_id},{offset} '
             f'num_images={len(image_urls)} finished, '
             f'ok={num_ok} error={num_error} cost={cost_ms:.0f}ms')
    await ctx.hope('harbor_rss.update_story_images', dict(
        feed_id=feed_id,
        offset=offset,
        story_url=story_url,
        images=images,
    ))


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
        story = {}
        content = data['content']
        summary = data['summary']
        title = data['title']
        story['has_mathjax'] = data['has_mathjax']
        story['link'] = data['url']
        story['image_url'] = data['image_url']
        story['audio_url'] = data['audio_url']
        story['iframe_url'] = data['iframe_url']
        story['summary'] = summary
        story['content'] = content
        story['sentence_count'] = _compute_sentence_count(content)
        content_hash_base64 = compute_hash_base64(content, summary, title)
        story['title'] = title
        story['content_hash_base64'] = content_hash_base64
        story['unique_id'] = data['ident']
        story['author'] = data["author_name"]
        dt_published = data['dt_published']
        dt_updated = data['dt_updated']
        story['dt_published'] = min(dt_published or dt_updated or now, now)
        story['dt_updated'] = min(dt_updated or dt_published or now, now)
        storys.append(story)
    return storys


def _compute_sentence_count(content: str) -> int:
    return len(split_sentences(story_html_to_text(content)))
