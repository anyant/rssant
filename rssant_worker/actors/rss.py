import logging
import asyncio
import time
from urllib.parse import unquote
from collections import deque
import concurrent.futures

import yarl
from validr import T, Invalid
from attrdict import AttrDict
from django.utils import timezone
from readability import Document as ReadabilityDocument
from actorlib import actor, ActorContext

from rssant_feedlib.async_reader import AsyncFeedReader, FeedResponseStatus
from rssant_feedlib import FeedFinder, FeedReader, FeedParser
from rssant_feedlib.processor import StoryImageProcessor, story_html_to_text
from rssant_feedlib.blacklist import compile_url_blacklist

from rssant.helper.content_hash import compute_hash_base64
from rssant_api.models import FeedStatus
from rssant_api.helper import shorten
from rssant_common.validator import compiler


LOG = logging.getLogger(__name__)


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
    dt_published=T.datetime.optional,
    dt_updated=T.datetime.optional,
    summary=T.str.optional,
    content=T.str.optional,
)

FeedSchema = T.dict(
    url=T.url,
    title=T.str,
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
            LOG.warning(f'{ex}, feed={feed_info}, story={story_info}')
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

    finder = FeedFinder(url, message_handler=message_handler)
    found = finder.find()
    feed = _parse_found(found) if found else None
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
    content_hash_base64: T.str.optional,
    etag: T.str.optional,
    last_modified: T.str.optional,
):
    reader = FeedReader()
    params = dict(etag=etag, last_modified=last_modified)
    status_code, response = reader.read(url, **params)
    LOG.info(f'read feed#{feed_id} url={unquote(url)} status_code={status_code}')
    if status_code != 200 or not response:
        return
    new_hash = compute_hash_base64(response.content)
    if new_hash == content_hash_base64:
        LOG.info(f'feed#{feed_id} url={unquote(url)} not modified by compare content hash!')
        return
    LOG.info(f'parse feed#{feed_id} url={unquote(url)}')
    parsed = FeedParser.parse_response(response)
    if parsed.bozo:
        LOG.warning(f'failed parse feed#{feed_id} url={unquote(url)}: {parsed.bozo_exception}')
        return
    feed = _parse_found(parsed)
    ctx.tell('harbor_rss.update_feed', dict(feed_id=feed_id, feed=feed))


@actor('worker_rss.fetch_story')
async def do_fetch_story(
    ctx: ActorContext,
    story_id: T.int,
    url: T.url,
):
    LOG.info(f'fetch story#{story_id} url={unquote(url)} begin')
    async with AsyncFeedReader() as reader:
        status, response = await reader.read(url)
    if response and response.url:
        url = str(response.url)
    LOG.info(f'fetch story#{story_id} url={unquote(url)} status={status} finished')
    if not (response and status == 200):
        return
    if not response.rssant_text:
        LOG.error(f'story#{story_id} url={unquote(url)} response text is empty!')
        return
    await ctx.hope('worker_rss.process_story_webpage', dict(
        story_id=story_id,
        url=url,
        text=response.rssant_text,
    ))


@actor('worker_rss.process_story_webpage')
def do_process_story_webpage(
    ctx: ActorContext,
    story_id: T.int,
    url: T.url,
    text: T.str,
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
    doc = ReadabilityDocument(text)
    content = doc.summary()
    summary = shorten(story_html_to_text(content), width=300)
    if not summary:
        return
    ctx.hope('harbor_rss.update_story', dict(
        story_id=story_id,
        content=content,
        summary=summary,
        url=url,
    ))
    processer = StoryImageProcessor(url, content)
    image_indexs = processer.parse()
    image_urls = {str(yarl.URL(x.value)) for x in image_indexs}
    LOG.info(f'found story#{story_id} {unquote(url)} has {len(image_urls)} images')
    if image_urls:
        ctx.hope('worker_rss.detect_story_images', dict(
            story_id=story_id,
            story_url=url,
            image_urls=list(image_urls),
        ))


@actor('worker_rss.detect_story_images')
async def do_detect_story_images(
    ctx: ActorContext,
    story_id: T.int,
    story_url: T.url,
    image_urls: T.list(T.url).unique,
):
    LOG.info(f'detect story images story_id={story_id} num_images={len(image_urls)} begin')
    async with AsyncFeedReader(allow_non_webpage=True) as reader:
        async def _read(url):
            if is_referer_deny_url(url):
                return url, FeedResponseStatus.REFERER_DENY.value
            status, response = await reader.read(
                url,
                referer="https://rss.anyant.com/story/",
                ignore_content=True
            )
            return url, status
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


def _parse_found(parsed):
    feed = AttrDict()
    res = parsed.response
    feed.url = _get_url(res)
    feed.content_hash_base64 = compute_hash_base64(res.content)
    parsed_feed = parsed.feed
    feed.title = shorten(parsed_feed["title"], 200)
    link = parsed_feed["link"]
    if not link.startswith('http'):
        # 有些link属性不是URL，用author_detail的href代替
        # 例如：'http://www.cnblogs.com/grenet/'
        author_detail = parsed_feed['author_detail']
        if author_detail:
            link = author_detail['href']
    if not link.startswith('http'):
        link = feed.url
    feed.link = link
    feed.author = shorten(parsed_feed["author"], 200)
    feed.icon = parsed_feed["icon"] or parsed_feed["logo"]
    feed.description = parsed_feed["description"] or parsed_feed["subtitle"]
    feed.dt_updated = _get_dt_updated(parsed_feed)
    feed.etag = _get_etag(res)
    feed.last_modified = _get_last_modified(res)
    feed.encoding = res.encoding
    feed.version = shorten(parsed.version, 200)
    feed.storys = _get_storys(parsed.entries)
    return validate_feed(feed)


def _get_storys(entries):
    storys = deque(maxlen=300)  # limit num storys
    for data in entries:
        story = {}
        story['unique_id'] = shorten(_get_story_unique_id(data), 200)
        content = ''
        if data["content"]:
            content = "\n<br/>\n".join([x["value"] for x in data["content"]])
        if not content:
            content = data["description"]
        if not content:
            content = data["summary"]
        story['content'] = content
        summary = data["summary"]
        if not summary:
            summary = content
        # TODO: performance
        summary = shorten(story_html_to_text(summary), width=300)
        story['summary'] = summary
        story['link'] = data["link"]
        title = shorten(data["title"] or story['link'] or story['unique_id'], 200)
        content_hash_base64 = compute_hash_base64(content, summary, title)
        story['title'] = title
        story['content_hash_base64'] = content_hash_base64
        story['author'] = shorten(data["author"], 200)
        story['dt_published'] = _get_dt_published(data)
        story['dt_updated'] = _get_dt_updated(data)
        storys.append(story)
    return list(storys)


def _get_etag(response):
    return response.headers.get("ETag")


def _get_last_modified(response):
    return response.headers.get("Last-Modified")


def _get_url(response):
    return response.url


def _get_dt_published(data, default=None):
    t = data["published_parsed"] or data["updated_parsed"] or default
    if t and t > timezone.now():
        t = default
    return t


def _get_dt_updated(data, default=None):
    t = data["updated_parsed"] or data["published_parsed"] or default
    if t and t > timezone.now():
        t = default
    return t


def _get_story_unique_id(entry):
    unique_id = entry['id']
    if not unique_id:
        unique_id = entry['link']
    if not unique_id:
        unique_id = entry['title']
    return unique_id
