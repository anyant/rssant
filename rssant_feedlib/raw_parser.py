import typing
import json
from io import BytesIO

import atoma
import feedparser
from validr import mark_index, T

from rssant_common.validator import compiler
from .response import FeedResponse


feedparser.RESOLVE_RELATIVE_URIS = False
feedparser.SANITIZE_HTML = False


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
        link = feed.feed["link"]
        if not link.startswith('http'):
            # 有些link属性不是URL，用author_detail的href代替
            # 例如：'http://www.cnblogs.com/grenet/'
            author_detail = feed.feed['author_detail']
            if author_detail:
                link = author_detail['href']
        return link

    def _get_feed_title(self, feed: feedparser.FeedParserDict) -> str:
        return feed.feed["title"] or \
            feed.feed["subtitle"] or \
            feed.feed["description"]

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
        if item["content"]:
            # both content and summary will in content list, peek the longest
            for x in item["content"]:
                value = x["value"]
                if value and len(value) > len(content):
                    content = value
        if not content:
            content = item["description"]
        if not content:
            content = item["summary"]
        return content

    def _extract_story(self, item):
        story = {}
        content = self._get_story_content(item)
        summary = item["summary"] if item["summary"] != content else None
        story['content'] = content
        story['summary'] = summary
        url = item["link"]
        title = item["title"]
        unique_id = item['id'] or url or title
        story['ident'] = unique_id
        story['url'] = url
        story['title'] = title
        story['image_url'] = self._get_story_image_url(item)
        story['dt_published'] = item["published_parsed"]
        story['dt_updated'] = item["updated_parsed"]
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
        for item in feed.items or []:
            ident = item.id_ or item.url or item.title
            content = item.content_html or item.content_text or item.summary
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
        if response.is_json:
            return self._parse_json_feed(response)
        warning = []
        if response.is_html:
            warning.append('feed content type is html')
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
        icon_url = feed.feed["icon"] or feed.feed["logo"]
        description = feed.feed["description"] or feed.feed["subtitle"]
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
        for item in feed.entries:
            storys.append(self._extract_story(item))
        result = RawFeedResult(feed_info, storys, warning=warning)
        return result

    def parse(self, response: FeedResponse) -> RawFeedResult:
        """初步解析Feed，返回标准化结构"""
        result = self._parse(response)
        if self._validate:
            result = self._validate_result(result)
        return result
