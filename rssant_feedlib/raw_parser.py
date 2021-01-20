import typing
import json
import logging
import datetime
import time
from io import BytesIO

import atoma
from atoma.json_feed import JSONFeedItem as RawJSONFeedItem
import feedparser
from django.utils import timezone
from dateutil.parser import parse as parse_datetime
from validr import mark_index, T, Invalid

from rssant_common.validator import compiler
from .response import FeedResponse


LOG = logging.getLogger(__name__)


feedparser.RESOLVE_RELATIVE_URIS = False
feedparser.SANITIZE_HTML = False

UTC = datetime.timezone.utc

# TODO: maybe remove in the future
# On the date story ident change to v2 format
STORY_INDENT_V2_DATE = datetime.datetime(2020, 9, 1, 0, 0, 0, tzinfo=UTC)
# On the date story ident change to v3 format
STORY_INDENT_V3_DATE = datetime.datetime(2020, 11, 17, 0, 0, 0, tzinfo=UTC)

RawFeedSchema = T.dict(
    version=T.str,
    title=T.str,
    url=T.str,
    home_url=T.str.optional,
    icon_url=T.str.optional,
    description=T.str.optional,
    dt_updated=T.datetime.object.optional,
    author_name=T.str.optional,
    author_url=T.str.optional,
    author_avatar_url=T.str.optional,
)


_MAX_CONTENT_LENGTH = 1000 * 1024
_MAX_SUMMARY_LENGTH = 10 * 1024


RawStorySchema = T.dict(
    ident=T.str,
    title=T.str,
    url=T.str.optional,
    content=T.str.maxlen(_MAX_CONTENT_LENGTH).optional,
    summary=T.str.maxlen(_MAX_SUMMARY_LENGTH).optional,
    image_url=T.str.optional,
    audio_url=T.str.optional,
    dt_published=T.datetime.object.optional,
    dt_updated=T.datetime.object.optional,
    author_name=T.str.optional,
    author_url=T.str.optional,
    author_avatar_url=T.str.optional,
)


validate_raw_feed = compiler.compile(RawFeedSchema)
validate_raw_story = compiler.compile(RawStorySchema)


class RawFeedResult:

    __slots__ = ('_feed', '_storys', '_warnings')

    def __init__(self, feed, storys, warnings=None):
        self._feed = feed
        self._storys = storys
        self._warnings = warnings

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
    def warnings(self) -> typing.List[str]:
        return self._warnings


class FeedParserError(Exception):
    """FeedParserError"""


class RawFeedParser:

    def __init__(self, validate=True):
        self._validate = validate

    def _get_feed_home_url(self, feed: feedparser.FeedParserDict) -> str:
        link = feed.feed.get("link") or ''
        if not link.startswith('http') and not link.startswith('/'):
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

    def _get_story_enclosure_url(self, item, mime_type) -> str:
        if item.get('enclosures'):
            for e in item['enclosures']:
                el_mime_type = e.get('type')
                url = e.get('href')
                if url and el_mime_type and mime_type in el_mime_type:
                    return url
        return None

    def _get_story_image_url(self, item) -> str:
        image = item.get('image')
        if image:
            url = image.get('href')
            if url:
                return url
        return self._get_story_enclosure_url(item, 'image')

    def _get_story_audio_url(self, item) -> str:
        return self._get_story_enclosure_url(item, 'audio')

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

    def _normalize_date(self, value) -> datetime.datetime:
        if not value:
            return None
        try:
            if isinstance(value, str) and value.isnumeric():
                value = datetime.datetime.fromtimestamp(int(value), tz=UTC)
            if isinstance(value, list) and len(value) == 9:
                value = tuple(value)
            if isinstance(value, tuple):
                try:
                    timestamp = time.mktime(value)
                except OverflowError:
                    return None
                value = datetime.datetime.fromtimestamp(timestamp, tz=UTC)
            elif not isinstance(value, datetime.datetime):
                value = parse_datetime(value)
                if value is None:
                    return None
        except Exception as ex:
            LOG.warning('normalize date failed, value=%r: %s', value, ex)
            return None
        if not timezone.is_aware(value):
            value = timezone.make_aware(value, timezone=UTC)
        # https://bugs.python.org/issue13305
        if value.year < 1000:
            return None
        if value.year > 2999:
            return None
        return value

    def _normalize_story_content(self, content, story_key):
        if content and len(content) > _MAX_CONTENT_LENGTH:
            msg = 'story %r content length=%s too large, will truncate it'
            LOG.warning(msg, story_key, len(content))
            content = content[:_MAX_CONTENT_LENGTH]
        return content or ''

    def _normalize_story_summary(self, summary, story_key):
        if summary and len(summary) > _MAX_SUMMARY_LENGTH:
            msg = 'story %r summary length=%s too large, will discard it'
            LOG.warning(msg, story_key, len(summary))
            summary = ''
        return summary or ''

    def _normalize_story_content_summary(self, story: dict) -> dict:
        _story_key = story['url'] or story['ident']
        content = story['content']
        summary = story['summary']
        if content == summary:
            summary = ''
        content = self._normalize_story_content(content, _story_key)
        summary = self._normalize_story_summary(summary, _story_key)
        story['content'] = content
        story['summary'] = summary
        return story

    @classmethod
    def _extract_story_ident_v1(cls, guid, title, link) -> str:
        """
        >>> RawFeedParser._extract_story_ident_v1('guid', 'title', 'http://example.com/')
        'guid'
        """
        return guid or link or title or ''

    @classmethod
    def _extract_story_ident_v2(cls, guid, title, link) -> str:
        r"""
        >>> RawFeedParser._extract_story_ident_v2('', '', '') == ''
        True
        >>> RawFeedParser._extract_story_ident_v2('guid', 'title', '')
        'guid'
        >>> RawFeedParser._extract_story_ident_v2('guid', 'title', 'http://example.com/')
        'http://example.com/::guid'
        >>> RawFeedParser._extract_story_ident_v2('guid', 'title', 'http://example.com/page.html#abc')
        'http://example.com/page.html::guid'
        >>> link = 'http://example.com/'
        >>> RawFeedParser._extract_story_ident_v2(link, 'title', link)
        'http://example.com/'
        """
        # strip link hash part to improve uniqueness
        link = link.rsplit('#', 1)[0]
        # guid may duplicate in some bad rss feed
        # eg: https://www.lieyunwang.com/newrss/feed.xml
        if guid and link and guid != link:
            ident = link + '::' + guid
        else:
            ident = guid or link or title or ''
        return ident

    @classmethod
    def _extract_story_ident_v3(cls, guid, title, link) -> str:
        r"""
        >>> RawFeedParser._extract_story_ident_v3('', '', '')
        ''
        >>> RawFeedParser._extract_story_ident_v3('guid', 'title', '')
        'guid::title'
        >>> RawFeedParser._extract_story_ident_v3('guid', 'title', 'http://example.com/')
        'guid::title'
        >>> RawFeedParser._extract_story_ident_v3('guid', '', 'link')
        'guid::'
        >>> link = 'http://example.com/page.html#abc'
        >>> RawFeedParser._extract_story_ident_v3('', 'title', link)
        'http://example.com/page.html'
        >>> link = 'http://example.com/page.html'
        >>> RawFeedParser._extract_story_ident_v3(link, 'title', link)
        'http://example.com/page.html::title'
        """
        # strip link hash part to improve uniqueness
        link = link.rsplit('#', 1)[0]
        # guid may duplicate in some bad rss feed
        #   eg: https://www.lieyunwang.com/newrss/feed.xml
        # and link may change in some feed when story not change
        #   eg: https://rsshub.app/coolapk/user/727333/dynamic
        #       https://rsshub.app/coolapk/tuwen
        #       https://github.com/DIYgod/RSSHub/issues/4523
        #       https://github.com/DIYgod/RSSHub/issues/6015
        if guid:
            ident = guid + '::' + title
        else:
            ident = link or title or ''
        return ident

    @classmethod
    def _strip_string(cls, s) -> str:
        r"""
        >>> RawFeedParser._strip_string(None) == ''
        True
        >>> RawFeedParser._strip_string(' hello\nworld ')
        'hello world'
        """
        return (s or '').replace('\n', ' ').strip()

    @classmethod
    def _get_story_ident_func(cls, dt: datetime.datetime, *, is_json_feed: bool) -> callable:
        """
        >>> RawFeedParser._get_story_ident_func(None, is_json_feed=False) \
            == RawFeedParser._extract_story_ident_v3
        True
        >>> RawFeedParser._get_story_ident_func(STORY_INDENT_V3_DATE, is_json_feed=False) \
            == RawFeedParser._extract_story_ident_v3
        True
        >>> RawFeedParser._get_story_ident_func(STORY_INDENT_V2_DATE, is_json_feed=False) \
            == RawFeedParser._extract_story_ident_v2
        True
        >>> dt = STORY_INDENT_V2_DATE - datetime.timedelta(days=1)
        >>> RawFeedParser._get_story_ident_func(dt, is_json_feed=False) == RawFeedParser._extract_story_ident_v1
        True
        >>> RawFeedParser._get_story_ident_func(None, is_json_feed=True) == RawFeedParser._extract_story_ident_v3
        True
        >>> RawFeedParser._get_story_ident_func(STORY_INDENT_V3_DATE, is_json_feed=True) \
            == RawFeedParser._extract_story_ident_v3
        True
        >>> dt = STORY_INDENT_V3_DATE - datetime.timedelta(days=1)
        >>> RawFeedParser._get_story_ident_func(dt, is_json_feed=True) == RawFeedParser._extract_story_ident_v1
        True
        """
        if is_json_feed:
            if dt and dt < STORY_INDENT_V3_DATE:
                return cls._extract_story_ident_v1
            else:
                return cls._extract_story_ident_v3
        else:
            if dt and dt < STORY_INDENT_V2_DATE:
                return cls._extract_story_ident_v1
            elif dt and dt < STORY_INDENT_V3_DATE:
                return cls._extract_story_ident_v2
            else:
                return cls._extract_story_ident_v3

    def _extract_story(self, item):
        story = {}
        url = self._strip_string(item.get("link"))
        title = self._strip_string(item.get("title"))
        guid = self._strip_string(item.get('id'))
        dt_published = self._get_date(item, 'published')
        dt_updated = self._get_date(item, 'updated')
        ident_func = self._get_story_ident_func(
            dt_published or dt_updated, is_json_feed=False)
        ident = ident_func(guid, title, url)
        if not ident:
            return None
        story['ident'] = ident
        story['url'] = url
        story['title'] = title or ident
        story['content'] = self._get_story_content(item)
        story['summary'] = item.get("summary")
        story['image_url'] = self._get_story_image_url(item)
        story['audio_url'] = self._get_story_audio_url(item)
        story['dt_published'] = dt_published
        story['dt_updated'] = dt_updated
        story.update(self._get_author_info(item))
        story = self._normalize_story_content_summary(story)
        return story

    def _get_date(self, item, name):
        """
        Fix feedparser.py:345: DeprecationWarning:
            To avoid breaking existing software while fixing issue 310,
            a temporary mapping has been created if `updated_parsed` doesn't exist.
            This fallback will be removed in a future version of feedparser.
        """
        value = None
        if f'{name}_parsed' in item:
            value = item.get(f'{name}_parsed')
        if (not value) and name in item:
            # support some feed which use unix timestamp date string
            # eg: http://tuijian.blogchina.com/home/headline
            value = item.get(name)
            if (not value) or (not value.isnumeric()):
                value = None
        return self._normalize_date(value)

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

    def _get_json_feed_story_audio_url(self, item: RawJSONFeedItem) -> str:
        if not item.attachments:
            return None
        for x in item.attachments:
            if x.url and x.mime_type and 'audio' in x.mime_type:
                return x.url
        return None

    def _get_json_feed_story(self, item: RawJSONFeedItem):
        dt_published = self._normalize_date(item.date_published)
        dt_updated = self._normalize_date(item.date_modified)
        guid = self._strip_string(item.id_)
        title = self._strip_string(item.title)
        url = self._strip_string(item.url)
        ident_func = self._get_story_ident_func(
            dt_published or dt_updated, is_json_feed=True)
        ident = ident_func(guid, title, url)
        if not ident:
            return None
        content = item.content_html or item.content_text or item.summary or ''
        summary = item.summary if item.summary != content else None
        audio_url = self._get_json_feed_story_audio_url(item)
        story = dict(
            ident=ident,
            url=url,
            title=title or ident,
            content=content,
            summary=summary,
            image_url=item.image or item.banner_image,
            audio_url=audio_url,
            dt_published=dt_published,
            dt_updated=dt_updated,
            **self._get_json_feed_author(item.author),
        )
        story = self._normalize_story_content_summary(story)
        return story

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
            dt_updated=None,
            icon_url=feed.icon or feed.favicon,
            **self._get_json_feed_author(feed.author),
        )
        warnings = []
        storys = []
        for i, item in enumerate(feed.items or []):
            story = self._get_json_feed_story(item)
            if not story:
                warnings.append(f"story#{i} no id, skip it")
                continue
            storys.append(story)
        if (not storys) and warnings:
            raise FeedParserError('; '.join(warnings))
        result = RawFeedResult(feed_info, storys, warnings=warnings)
        return result

    def _validate_result(self, result: RawFeedResult) -> RawFeedResult:
        storys = []
        try:
            feed = validate_raw_feed(result.feed)
            for i, s in enumerate(result.storys):
                with mark_index(i):
                    s = validate_raw_story(s)
                    storys.append(s)
        except Invalid as ex:
            raise FeedParserError(str(ex)) from ex
        return RawFeedResult(feed, storys, warnings=result.warnings)

    def _parse(self, response: FeedResponse) -> RawFeedResult:
        assert response.ok and response.content
        if response.feed_type.is_json:
            return self._parse_json_feed(response)
        warnings = []
        if response.feed_type.is_html:
            warnings.append('feed content type is html')
        if response.feed_type.is_other:
            warnings.append('feed content type is not any feed type')
        # content.strip is required because feedparser not allow whitespace
        stream = BytesIO(response.content.strip())
        # tell feedparser to use detected encoding
        headers = {
            'content-type': f'application/xml;charset={response.encoding}',
        }
        feed = feedparser.parse(stream, response_headers=headers)
        if feed.bozo:
            ex = feed.get("bozo_exception")
            if ex:
                name = type(ex).__module__ + "." + type(ex).__name__
                warnings.append(f"{name}: {ex}")
        feed_version = feed.get("version")
        if not feed_version:
            warnings.append('feed version unknown')
        feed_title = self._get_feed_title(feed)
        if not feed_title:
            warnings.append("feed no title")
        has_entries = len(feed.entries) > 0
        if not has_entries:
            warnings.append("feed not contain any entries")
        # totally bad feed, raise an error
        if (not has_entries) and warnings:
            raise FeedParserError('; '.join(warnings))
        # extract feed info
        icon_url = feed.feed.get("icon") or feed.feed.get("logo")
        description = feed.feed.get("description") or feed.feed.get("subtitle")
        dt_updated = self._get_date(feed.feed, 'updated') or \
            self._get_date(feed.feed, 'published')
        feed_info = dict(
            version=feed_version,
            title=feed_title,
            url=response.url,
            home_url=self._get_feed_home_url(feed),
            icon_url=icon_url,
            description=description,
            dt_updated=dt_updated,
            **self._get_author_info(feed.feed),
        )
        # extract storys info
        storys = []
        for i, item in enumerate(feed.entries):
            story = self._extract_story(item)
            if not story:
                warnings.append(f"story#{i} no id, skip it")
                continue
            storys.append(story)
        if (not storys) and warnings:
            raise FeedParserError('; '.join(warnings))
        result = RawFeedResult(feed_info, storys, warnings=warnings)
        return result

    def parse(self, response: FeedResponse) -> RawFeedResult:
        """初步解析Feed，返回标准化结构"""
        result = self._parse(response)
        if self._validate:
            result = self._validate_result(result)
        return result
