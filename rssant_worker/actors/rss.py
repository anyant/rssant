import logging
import asyncio
import time
from urllib.parse import unquote
import concurrent.futures

from validr import T, Invalid
from attrdict import AttrDict
from django.utils import timezone

from actorlib import actor, ActorContext

from rssant_feedlib import AsyncFeedReader, FeedResponseStatus
from rssant_feedlib import (
    FeedFinder, FeedReader,
    FeedParser, RawFeedParser,
    RawFeedResult, FeedResponse, FeedParserError,
)
from rssant_feedlib.processor import (
    story_readability, story_html_to_text, story_html_clean,
    process_story_links
)
from rssant_feedlib.blacklist import compile_url_blacklist
from rssant_feedlib.fulltext import is_fulltext_content, split_sentences

from rssant.helper.content_hash import compute_hash_base64
from rssant_api.models import FeedStatus
from rssant_api.helper import shorten
from rssant_common.validator import compiler
from rssant_config import CONFIG


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
    has_mathjax=T.bool.optional,
    dt_published=T.datetime.optional,
    dt_updated=T.datetime.optional,
    summary=T.str.optional,
    content=T.str.optional,
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


def _get_proxy_options():
    options = {}
    if CONFIG.rss_proxy_enable:
        options.update(
            rss_proxy_url=CONFIG.rss_proxy_url,
            rss_proxy_token=CONFIG.rss_proxy_token,
        )
    return options


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

    options = dict(message_handler=message_handler, **_get_proxy_options())
    options.update(allow_private_address=CONFIG.allow_private_address)
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
    content_hash_base64: T.str.optional,
    etag: T.str.optional,
    last_modified: T.str.optional,
):
    params = dict(etag=etag, last_modified=last_modified, use_proxy=use_proxy)
    options = _get_proxy_options()
    options.update(allow_private_address=CONFIG.allow_private_address)
    with FeedReader(**options) as reader:
        response = reader.read(url, **params)
    LOG.info(f'read feed#{feed_id} url={unquote(url)} response.status={response.status}')
    if response.status != 200 or not response.content:
        return
    new_hash = compute_hash_base64(response.content)
    if new_hash == content_hash_base64:
        LOG.info(f'feed#{feed_id} url={unquote(url)} not modified by compare content hash!')
        return
    LOG.info(f'parse feed#{feed_id} url={unquote(url)}')
    try:
        raw_result = RawFeedParser().parse(response)
    except FeedParserError as ex:
        LOG.warning('failed parse feed#%s url=%r: %s', feed_id, unquote(url), ex)
        return
    if raw_result.warnings:
        warnings = '; '.join(raw_result.warnings)
        LOG.warning('warning parse feed#%s url=%r: %s', feed_id, unquote(url), warnings)
        return
    try:
        feed = _parse_found((response, raw_result))
    except (Invalid, FeedParserError) as ex:
        LOG.error('invalid feed#%s url=%r: %s', feed_id, unquote(url), ex, exc_info=ex)
        return
    ctx.tell('harbor_rss.update_feed', dict(feed_id=feed_id, feed=feed))


@actor('worker_rss.fetch_story')
async def do_fetch_story(
    ctx: ActorContext,
    story_id: T.int,
    url: T.url,
    use_proxy: T.bool.default(False),
    num_sub_sentences: T.int.optional,
):
    LOG.info(f'fetch story#{story_id} url={unquote(url)} begin')
    options = _get_proxy_options()
    options.update(allow_private_address=CONFIG.allow_private_address)
    async with AsyncFeedReader(**options) as reader:
        use_proxy = use_proxy and reader.has_rss_proxy
        response = await reader.read(url, use_proxy=use_proxy)
    if response and response.url:
        url = str(response.url)
    LOG.info(f'fetch story#{story_id} url={unquote(url)} status={response.status} finished')
    if not (response and response.ok):
        return
    if not response.content:
        msg = 'story#%s url=%s response text is empty!'
        LOG.error(msg, story_id, unquote(url))
        return
    try:
        content = response.content.decode(response.encoding)
    except UnicodeDecodeError as ex:
        LOG.warning('fetch story unicode decode error=%s url=%r', ex, url)
        content = response.content.decode(response.encoding, errors='ignore')
    if len(content) >= _MAX_STORY_HTML_LENGTH:
        content = story_html_clean(content)
        if len(content) >= _MAX_STORY_HTML_LENGTH:
            msg = 'too large story#%s size=%s url=%r'
            LOG.warning(msg, story_id, len(content), url)
            content = story_html_to_text(content)[:_MAX_STORY_HTML_LENGTH]
    await ctx.hope('worker_rss.process_story_webpage', dict(
        story_id=story_id,
        url=url,
        text=content,
        num_sub_sentences=num_sub_sentences,
    ))


@actor('worker_rss.process_story_webpage')
def do_process_story_webpage(
    ctx: ActorContext,
    story_id: T.int,
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
    if len(content) > _MAX_STORY_CONTENT_LENGTH:
        msg = 'too large story#%s size=%s url=%r, will only save plain text'
        LOG.warning(msg, story_id, len(content), url)
        content = shorten(story_html_to_text(content), width=_MAX_STORY_CONTENT_LENGTH)
    # 如果取回的内容比RSS内容更短，就不是正确的全文
    if num_sub_sentences is not None:
        if not is_fulltext_content(content):
            num_sentences = len(split_sentences(story_html_to_text(content)))
            if num_sentences <= num_sub_sentences:
                msg = 'fetched story#%s url=%s num_sentences=%s less than num_sub_sentences=%s'
                LOG.info(msg, story_id, url, num_sentences, num_sub_sentences)
                return
    summary = shorten(story_html_to_text(content), width=_MAX_STORY_SUMMARY_LENGTH)
    if not summary:
        return
    ctx.hope('harbor_rss.update_story', dict(
        story_id=story_id,
        content=content,
        summary=summary,
        url=url,
    ))


@actor('worker_rss.detect_story_images')
async def do_detect_story_images(
    ctx: ActorContext,
    story_id: T.int,
    story_url: T.url,
    image_urls: T.list(T.url).unique,
):
    LOG.info(f'detect story images story_id={story_id} num_images={len(image_urls)} begin')
    options = dict(
        allow_non_webpage=True,
        allow_private_address=CONFIG.allow_private_address,
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
    LOG.info(f'detect story images story_id={story_id} '
             f'num_images={len(image_urls)} finished, '
             f'ok={num_ok} error={num_error} cost={cost_ms:.0f}ms')
    await ctx.hope('harbor_rss.update_story_images', dict(
        story_id=story_id,
        story_url=story_url,
        images=images,
    ))


def _parse_found(found):
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
    del found, response  # release memory in advance

    # parse feed and storys
    result = FeedParser().parse(raw_result)
    del raw_result  # release memory in advance

    feed.title = result.feed['title']
    feed.link = result.feed['home_url']
    feed.author = result.feed['author_name']
    feed.icon = result.feed['icon_url']
    feed.description = result.feed['description']
    feed.dt_updated = result.feed['dt_updated']
    feed.version = result.feed['version']
    feed.storys = _get_storys(result.storys)
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
        story['summary'] = summary
        story['content'] = content
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
