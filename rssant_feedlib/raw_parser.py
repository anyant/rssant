import typing
import json
import logging
import datetime
import time
from io import BytesIO

import atoma
import feedparser
from django.utils import timezone
from dateutil.parser import parse as parse_datetime
from validr import mark_index, T

from rssant_common.validator import compiler
from .response import FeedResponse


LOG = logging.getLogger(__name__)


feedparser.RESOLVE_RELATIVE_URIS = False
feedparser.SANITIZE_HTML = False

UTC = datetime.timezone.utc

RawFeedSchema = T.dict(
    version=T.str,
    title=T.str,
    url=T.str,
    home_url=T.str.optional,
    icon_url=T.str.optional,
    description=T.str.optional,
    author_name=T.str.optional,
    author_url=T.str.optional,
    author_avatar_url=T.str.optional,
)


RawStorySchema = T.dict(
    ident=T.str,
    title=T.str,
    url=T.str.optional,
    content=T.str.optional,
    summary=T.str.optional,
    image_url=T.str.optional,
    dt_published=T.datetime.optional,
    dt_updated=T.datetime.optional,
    author_name=T.str.optional,
    author_url=T.str.optional,
    author_avatar_url=T.str.optional,
)


validate_raw_feed = compiler.compile(RawFeedSchema)
validate_raw_story = compiler.compile(RawStorySchema)


class RawFeedResult:

    __slots__ = ('_feed', '_storys', '_warning')

    def __init__(self, feed, storys, warning=None):
        self._feed = feed
        self._storys = storys
        self._warning = warning

    def __repr__(self):
        return '<{} url={!r} version={!r} title={!r} has {} storys>'.format(
            type(self).__name__,
            self.feed['url'],
            self.feed['version'],
            self.feed['title'],
            len(self.storys),
        )

    @property
    def feed(self) -> RawFeedSchema:
        return self._feed

    @property
    def storys(self) -> typing.List[RawStorySchema]:
        return self._storys

    @property
    def warning(self) -> str:
        return self._warning


class FeedParserError(Exception):
    """FeedParserError"""


class RawFeedParser:

    def __init__(self, validate=False):
        self._validate = validate

    def _get_feed_home_url(self, feed: feedparser.FeedParserDict) -> str:
        link = feed.feed.get("link")
        if not link.startswith('http'):
            # 有些link属性不是URL，用author_detail的href代替
            # 例如：'http://www.cnblogs.com/grenet/'
            author_detail = feed.feed.get('author_detail')
            if author_detail:
                link = author_detail.get('href')
        return link

    def _get_feed_title(self, feed: feedparser.FeedParserDict) -> str:
        return feed.feed.get("title") or \
            feed.feed.get("subtitle") or \
            feed.feed.get("description")

    def _get_author_info(self, item: dict) -> dict:
        detail = item.get('author_detail')
        name = item.get('author')
        url = avatar = None
        if detail:
            name = detail.get('name') or name
            url = detail.get('href')
        return dict(author_name=name, author_url=url, author_avatar_url=avatar)

    def _get_story_image_url(self, item) -> str:
        if item.get('enclosures'):
            for e in item['enclosures']:
                mime_type = e.get('type')
                url = e.get('href')
                if url and mime_type and 'image' in mime_type:
                    return url
        return None

    def _get_story_content(self, item) -> str:
        content = ''
        if item.get("content"):
            # both content and summary will in content list, peek the longest
            for x in item["content"]:
                value = x.get("value")
                if value and len(value) > len(content):
                    content = value
        if not content:
            content = item.get("description")
        if not content:
            content = item.get("summary")
        return content or ''

    def _normlize_date(self, value) -> datetime.datetime:
        if not value:
            return None
        if isinstance(value, list) and len(value) == 9:
            value = tuple(value)
        if isinstance(value, tuple):
            value = datetime.datetime.fromtimestamp(time.mktime(value), tz=UTC)
        elif not isinstance(value, datetime.datetime):
            value = parse_datetime(value)
            if value is None:
                return None
        if not timezone.is_aware(value):
            value = timezone.make_aware(value, timezone=UTC)
        # https://bugs.python.org/issue13305
        if value.year < 1000:
            return None
        return value

    def _extract_story(self, item):
        story = {}
        content = self._get_story_content(item)
        summary = item.get("summary")
        if summary == content:
            summary = None
        story['content'] = content or ''
        story['summary'] = summary or ''
        url = item.get("link")
        title = item.get("title")
        unique_id = item.get('id') or url or title
        if not unique_id:
            return None
        story['ident'] = unique_id
        story['url'] = url
        story['title'] = title
        story['image_url'] = self._get_story_image_url(item)
        story['dt_published'] = self._normlize_date(item.get("published_parsed"))
        story['dt_updated'] = self._normlize_date(item.get("updated_parsed"))
        story.update(self._get_author_info(item))
        return story

    def _get_json_feed_author(self, author):
        name = url = avatar = None
        if author:
            name = author.name
            url = author.url
            avatar = author.avatar
        return dict(author_name=name, author_url=url, author_avatar_url=avatar)

    def _load_json(self, response: FeedResponse) -> dict:
        try:
            text = response.content.decode(response.encoding)
        except UnicodeDecodeError as ex:
            raise FeedParserError("Unicode decode error: {}".format(ex)) from ex
        try:
            data = json.loads(text)
        except json.JSONDecodeError as ex:
            raise FeedParserError("JSON parse error: {}".format(ex)) from ex
        return data

    def _parse_json_feed(self, response: FeedResponse) -> RawFeedResult:
        data = self._load_json(response)
        if not isinstance(data, dict):
            raise FeedParserError("JSON feed data should be dict")
        try:
            feed: atoma.JSONFeed = atoma.parse_json_feed(data)
        except atoma.FeedParseError as ex:
            raise FeedParserError(str(ex)) from ex
        feed_info = dict(
            version=feed.version,
            title=feed.title,
            url=response.url,
            home_url=feed.home_page_url,
            description=feed.description,
            icon_url=feed.icon or feed.favicon,
            **self._get_json_feed_author(feed.author),
        )
        storys = []
        item: atoma.JSONFeedItem
        for i, item in enumerate(feed.items or []):
            ident = item.id_ or item.url or item.title
            if not ident:
                LOG.warning("feed %s story#%s no ident, skip it", response.url, i)
                continue
            content = item.content_html or item.content_text or item.summary or ''
            summary = item.summary if item.summary != content else None
            story = dict(
                ident=ident,
                url=item.url,
                title=item.title,
                content=content,
                summary=summary,
                image_url=item.image or item.banner_image,
                dt_published=item.date_published,
                dt_updated=item.date_modified,
                **self._get_json_feed_author(item.author),
            )
            storys.append(story)
        result = RawFeedResult(feed_info, storys)
        return result

    def _validate_result(self, result: RawFeedResult) -> RawFeedResult:
        feed = validate_raw_feed(result.feed)
        storys = []
        for i, s in enumerate(result.storys):
            with mark_index(i):
                s = validate_raw_story(s)
                storys.append(s)
        return RawFeedResult(feed, storys, warning=result.warning)

    def _parse(self, response: FeedResponse) -> RawFeedResult:
        assert response.ok and response.content
        if response.feed_type.is_json:
            return self._parse_json_feed(response)
        warning = []
        if response.feed_type.is_html:
            warning.append('feed content type is html')
        if response.feed_type.is_other:
            warning.append('feed content type is not any feed type')
        stream = BytesIO(response.content)
        # tell feedparser to use detected encoding
        headers = {
            'content-type': f'application/xml;charset={response.encoding}',
        }
        feed = feedparser.parse(stream, response_headers=headers)
        if feed.bozo:
            ex = feed.get("bozo_exception")
            if ex:
                name = type(ex).__module__ + "." + type(ex).__name__
                warning.append(f"{name}: {ex}")
        feed_version = feed.get("version")
        if not feed_version:
            warning.append('feed version unknown')
        feed_title = self._get_feed_title(feed)
        if not feed_title:
            warning.append("feed no title")
        has_entries = len(feed.entries) > 0
        if not has_entries:
            warning.append("feed not contain any entries")
        warning = '; '.join(warning)
        # totally bad feed, raise an error
        if (not has_entries) and warning:
            raise FeedParserError(warning)
        # extract feed info
        icon_url = feed.feed.get("icon") or feed.feed.get("logo")
        description = feed.feed.get("description") or feed.feed.get("subtitle")
        feed_info = dict(
            version=feed_version,
            title=feed_title,
            url=response.url,
            home_url=self._get_feed_home_url(feed),
            icon_url=icon_url,
            description=description,
            **self._get_author_info(feed.feed),
        )
        # extract storys info
        storys = []
        for i, item in enumerate(feed.entries):
            story = self._extract_story(item)
            if not story:
                LOG.warning("feed %s story#%s no ident, skip it", response.url, i)
                continue
            storys.append(story)
        result = RawFeedResult(feed_info, storys, warning=warning)
        return result

    def parse(self, response: FeedResponse) -> RawFeedResult:
        """初步解析Feed，返回标准化结构"""
        result = self._parse(response)
        if self._validate:
            result = self._validate_result(result)
        return result
