from urllib.parse import unquote
import logging

import celery
from celery import shared_task as task
from django.db import transaction
from django.utils import timezone
from readability import Document as ReadabilityDocument

from rssant_feedlib import FeedFinder, FeedReader, FeedParser
from rssant_feedlib.processor import StoryImageProcessor, story_html_to_text
from rssant.helper.content_hash import compute_hash_base64
from rssant_api.models import UserFeed, RawFeed, Feed, Story, FeedUrlMap, FeedStatus, FeedCreation
from rssant_api.helper import shorten
from rssant_common.image_url import encode_image_url
from rssant_common.async_client import async_client


LOG = logging.getLogger(__name__)


@task(name='rssant.tasks.find_feed')
def find_feed(feed_creation_id):
    messages = []

    def message_handler(msg):
        LOG.info(msg)
        messages.append(msg)

    feed_creation = FeedCreation.get_by_pk(feed_creation_id)
    feed_creation.status = FeedStatus.UPDATING
    feed_creation.save()

    start_url = feed_creation.url
    finder = FeedFinder(start_url, message_handler=message_handler)
    found = finder.find()

    feed_creation.refresh_from_db()
    feed_creation.message = '\n\n'.join(messages)
    feed_creation.dt_updated = timezone.now()
    if not found:
        feed_creation.status = FeedStatus.ERROR
        feed_creation.save()
        FeedUrlMap(source=start_url, target=FeedUrlMap.NOT_FOUND).save()
    else:
        feed_creation.status = FeedStatus.READY
        feed_creation.save()
        _save_found(feed_creation, found)
    return {
        'user_id': feed_creation.user_id,
        'start_url': start_url,
        'messages': messages,
    }


@task(name='rssant.tasks.check_feed')
def check_feed(seconds=300):
    feed_ids = Feed.take_outdated(seconds)
    LOG.info('found {} feeds need sync'.format(len(feed_ids)))
    tasks = [sync_feed.s(feed_id=feed_id) for feed_id in feed_ids]
    celery.group(tasks).apply_async()
    return dict(feed_ids=feed_ids)


@task(name='rssant.tasks.sync_feed')
def sync_feed(feed_id):
    feed = Feed.get_by_pk(feed_id)
    LOG.info(f'read feed#{feed_id} url={feed.url}')
    status_code, response = _read_response(feed)
    LOG.info(f'feed#{feed_id} url={feed.url} status_code={status_code}')
    feed.refresh_from_db()
    feed.status = FeedStatus.READY
    feed.save()
    default_result = dict(
        feed_id=feed_id,
        url=feed.url,
        status_code=status_code,
        num_storys=0,
    )
    if status_code != 200 or not response:
        if status_code != 304:
            _create_raw_feed(feed, status_code, response)
        return default_result
    content_hash_base64 = compute_hash_base64(response.content)
    if not feed.is_modified(content_hash_base64):
        LOG.info(f'feed#{feed_id} url={feed.url} not modified by compare content hash!')
        return default_result
    LOG.info(f'parse feed#{feed_id} url={feed.url}')
    parsed = FeedParser.parse_response(response)
    if parsed.bozo:
        LOG.warning(f'failed parse feed#{feed_id} url={feed.url}: {parsed.bozo_exception}')
        return default_result
    num_modified, num_storys = _save_storys(feed, parsed.entries)
    feed.refresh_from_db()
    _save_feed(feed, parsed, content_hash_base64, has_update=num_modified > 0)
    if num_modified > 0:
        _create_raw_feed(feed, status_code, response, content_hash_base64=content_hash_base64)
    else:
        LOG.info(f'feed#{feed_id} url={feed.url} not modified by compare storys!')
    return dict(
        feed_id=feed_id,
        url=feed.url,
        status_code=status_code,
        num_storys=num_storys,
    )


@task(name='rssant.tasks.clean_feed_creation')
def clean_feed_creation():
    # 删除所有status=ERROR, 没有feed_id，并且入库时间超过2小时的订阅创建信息
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
        FeedStatus.PENDING, survival_seconds=30 * 60)
    num_retry_pending = len(feed_creation_ids)
    LOG.info('retry {} status=PENDING feed creations'.format(num_retry_pending))
    _retry_feed_creations(feed_creation_ids)
    return dict(
        num_deleted=num_deleted,
        num_retry_updating=num_retry_updating,
        num_retry_pending=num_retry_pending,
    )


@task(name='rssant.tasks.process_story_webpage')
def process_story_webpage(story_id):
    # https://github.com/dragnet-org/dragnet
    # https://github.com/misja/python-boilerpipe
    # https://github.com/dalab/web2text
    # https://github.com/grangier/python-goose
    # https://github.com/buriy/python-readability
    # https://github.com/codelucas/newspaper
    story = async_client.get_story(story_id)
    story_url = story['url']
    LOG.info(f'fetch story#{story_id} {story_url} status={story["status"]}')
    if not story['status'] == 200:
        return
    doc = ReadabilityDocument(story['text'])
    content = doc.summary()
    with transaction.atomic():
        story = Story.objects.get(pk=story_id)
        story.link = story_url
        story.content = content
        story.save()
    processer = StoryImageProcessor(story_url, content)
    image_indexs = processer.parse()
    img_urls = {x.value for x in image_indexs}
    LOG.info(f'found story#{story_id} {story_url} has {len(img_urls)} images')
    if img_urls:
        async_client.detect_story_images(
            story_id, story_url, img_urls,
            callback='/async_callback/story_images'
        )


IMAGE_REFERER_DENY_STATUS = set([400, 401, 403, 404])


@task(name='rssant.tasks.process_story_images')
def process_story_images(story_id, story_url, images):
    image_replaces = {}
    for img in images:
        if img['status'] in IMAGE_REFERER_DENY_STATUS:
            new_url_data = encode_image_url(img['url'], story_url)
            image_replaces[img['url']] = '/api/v1/image/{}'.format(new_url_data)
    LOG.info(f'detect story#{story_id} {story_url} '
             f'has {len(image_replaces)} referer deny images')
    story = Story.objects.get(pk=story_id)
    processor = StoryImageProcessor(story_url, story.content)
    image_indexs = processor.parse()
    content = processor.process(image_indexs, image_replaces)
    story.content = content
    story.save()


@transaction.atomic
def _save_found(feed_creation, parsed):
    url = _get_url(parsed.response)
    feed = Feed.get_first_by_url(url)
    if not feed:
        feed = Feed(url=url, status=FeedStatus.READY, dt_updated=timezone.now())
        feed.save()
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
    content_hash_base64 = compute_hash_base64(parsed.response.content)
    _save_feed(feed, parsed, content_hash_base64=content_hash_base64)
    _create_raw_feed(feed, parsed.response.status_code, parsed.response,
                     content_hash_base64=feed.content_hash_base64)
    _save_storys(feed, parsed.entries)
    FeedUrlMap(source=feed_creation.url, target=feed.url).save()
    if feed.link != feed_creation.url:
        FeedUrlMap(source=feed.link, target=feed.url).save()
    if feed.url != feed_creation.url:
        FeedUrlMap(source=feed.url, target=feed.url).save()


def _create_raw_feed(feed, status_code, response, content_hash_base64=None):
    raw_feed = RawFeed(feed=feed)
    raw_feed.url = feed.url
    raw_feed.status_code = status_code
    raw_feed.content_hash_base64 = content_hash_base64
    if response:
        raw_feed.encoding = response.encoding
        raw_feed.etag = _get_etag(response)
        raw_feed.last_modified = _get_last_modified(response)
        headers = {}
        for k, v in response.headers.items():
            headers[k.lower()] = v
        raw_feed.headers = headers
        raw_feed.set_content(response.content)
        raw_feed.content_length = len(response.content)
    raw_feed.save()
    return raw_feed


def _save_feed(feed, parsed, content_hash_base64=None, has_update=True):
    parsed_feed = parsed.feed
    res = parsed.response
    feed.url = _get_url(res)
    feed.title = shorten(parsed_feed["title"], 200)
    link = parsed_feed["link"]
    if not link.startswith('http'):
        # 有些link属性不是URL，用author_detail的href代替
        # 例如：'http://www.cnblogs.com/grenet/'
        author_detail = parsed_feed['author_detail']
        if author_detail:
            link = author_detail['href']
    feed.link = unquote(link)
    feed.author = shorten(parsed_feed["author"], 200)
    feed.icon = parsed_feed["icon"] or parsed_feed["logo"]
    feed.description = parsed_feed["description"] or parsed_feed["subtitle"]
    now = timezone.now()
    if has_update:
        feed.dt_updated = _get_dt_updated(parsed_feed, now)
    feed.dt_checked = feed.dt_synced = now
    feed.etag = _get_etag(res)
    feed.last_modified = _get_last_modified(res)
    feed.encoding = res.encoding
    feed.version = shorten(parsed.version, 200)
    feed.status = FeedStatus.READY
    feed.content_hash_base64 = content_hash_base64
    feed.save()
    return feed


def _save_storys(feed, entries):
    storys = []
    now = timezone.now()
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
        summary = shorten(summary, width=300)
        story['summary'] = summary
        title = shorten(data["title"], 200)
        content_hash_base64 = compute_hash_base64(content, summary, title)
        story['title'] = title
        story['content_hash_base64'] = content_hash_base64
        story['link'] = unquote(data["link"])
        story['author'] = shorten(data["author"], 200)
        story['dt_published'] = _get_dt_published(data, now)
        story['dt_updated'] = _get_dt_updated(data, now)
        storys.append(story)
    modified_storys, num_reallocate = Story.bulk_save_by_feed(feed.id, storys)
    LOG.info(
        'feed#%s save storys total=%s num_modified=%s num_reallocate=%s',
        feed.id, len(storys), len(modified_storys), num_reallocate
    )
    need_fetch_storys = []
    for story in modified_storys:
        story_text = story_html_to_text(story.content)
        if len(story_text) < 1000:
            need_fetch_storys.append({'id': str(story.id), 'url': story.link})
    if need_fetch_storys:
        LOG.info('feed#%s need fetch %s storys', feed.id, len(need_fetch_storys))
        try:
            async_client.fetch_storys(need_fetch_storys, '/async_callback/story')
        except Exception as ex:
            LOG.exception(f'async_client.fetch_storys failed: {ex}', exc_info=ex)
    return len(modified_storys), len(storys)


def _get_etag(response):
    return response.headers.get("ETag")


def _get_last_modified(response):
    return response.headers.get("Last-Modified")


def _get_url(response):
    return unquote(response.url)


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
    return unquote(unique_id)


def _read_response(feed):
    reader = FeedReader()
    status_code, response = reader.read(
        feed.url, etag=feed.etag, last_modified=feed.last_modified)
    return status_code, response


def _retry_feed_creations(feed_creation_ids):
    tasks = []
    for feed_creation_id in feed_creation_ids:
        tasks.append(find_feed.s(feed_creation_id))
    FeedCreation.bulk_set_pending(feed_creation_ids)
    celery.group(tasks).apply_async()
