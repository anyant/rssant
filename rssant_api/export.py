"""数据导出：收藏和单个订阅打包成 zip。

zip 结构：manifest.json、articles.json、index.xlsx、content/<序号>-<slug>.md；
单订阅额外有 feed.json。见任务 45156 的提案。
"""
import io
import json
import re
import tempfile
import zipfile
from datetime import datetime, timedelta
from datetime import timezone as dt_timezone

import html2text
from django.core.cache import cache
from django.core.exceptions import PermissionDenied
from django.http import FileResponse, JsonResponse
from openpyxl import Workbook

from rssant_api.models import STORY_SERVICE, Feed, StoryId, UnionFeed, UserFeed, UserStory
from rssant_common.validator import FeedUnionId
from rssant_config import CONFIG

EXPORT_FORMAT = 'rssant-zip-v1'
EXPORT_COOLDOWN_SECONDS = 60
FAVORITE_BATCH_SIZE = 200
FEED_BATCH_SIZE = 100
EXPORT_TIMEZONE = dt_timezone(timedelta(hours=8))
FILENAME_BAD_CHARS = re.compile(r'[/\\:*?"<>|\r\n\t]+')


def _format_dt(dt) -> str:
    if not dt:
        return ''
    return dt.astimezone(EXPORT_TIMEZONE).strftime('%Y-%m-%d %H:%M')


def _html_to_markdown(content: str) -> str:
    if not content:
        return ''
    parser = html2text.HTML2Text()
    parser.body_width = 0
    parser.ignore_images = False
    parser.ignore_links = False
    return parser.handle(content).strip()


def _slug(title: str, max_length: int = 40) -> str:
    title = FILENAME_BAD_CHARS.sub('-', (title or '').strip())
    title = re.sub(r'\s+', '-', title).strip('-. ')
    title = title[:max_length].strip('-. ')
    return title or 'story'


def _content_filename(index: int, title: str) -> str:
    return 'content/{:04d}-{}.md'.format(index, _slug(title))


def _story_markdown(story) -> str:
    title = story.title or '无标题'
    body = _html_to_markdown(story.content)
    if not body:
        body = _html_to_markdown(story.summary)
    if not body:
        body = '原文内容已不可用，请打开原文链接：{}'.format(story.link or '')
    return '# {}\n\n{}\n'.format(title, body)


def _story_article(story, feed_id, feed_title, feed_url, favorited_at, content_file: str) -> dict:
    return dict(
        id=StoryId.encode(story.feed_id, story.offset),
        title=story.title,
        url=story.link,
        author=story.author,
        feed=dict(
            id=feed_id,
            title=feed_title,
            url=feed_url,
        ),
        published_at=_format_dt(story.dt_published),
        favorited_at=_format_dt(favorited_at),
        summary=story.summary,
        image_url=story.image_url,
        audio_url=story.audio_url,
        iframe_url=story.iframe_url,
        unique_id=story.unique_id,
        content_missing=not bool((story.content or '').strip()),
        content_file=content_file,
    )


def _excel_text(value):
    if isinstance(value, str) and value[:1] in ('=', '+', '-', '@'):
        return "'" + value
    return value


def _write_xlsx(articles) -> bytes:
    workbook = Workbook(write_only=True)
    sheet = workbook.create_sheet('文章')
    sheet.append(['序号', '标题', '原文链接', '作者', '订阅源', '发布时间', '收藏时间', '正文是否缺失', '正文文件'])
    for index, item in enumerate(articles, start=1):
        sheet.append([
            index,
            _excel_text(item['title']),
            _excel_text(item['url']),
            _excel_text(item['author']),
            _excel_text(item['feed']['title']),
            item['published_at'],
            item['favorited_at'],
            '是' if item['content_missing'] else '否',
            item['content_file'],
        ])
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def _write_json(zf, name, data):
    zf.writestr(name, json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'))


def _build_zip(scope, user, articles, content_writer, extra_files=None):
    tmp = tempfile.TemporaryFile()
    with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as zf:
        for index, (article, story) in enumerate(articles, start=1):
            zf.writestr(article['content_file'], content_writer(story).encode('utf-8'))
        if extra_files:
            for name, data in extra_files:
                _write_json(zf, name, data)
        _write_json(zf, 'articles.json', [item for item, _ in articles])
        zf.writestr('index.xlsx', _write_xlsx([item for item, _ in articles]))
        _write_json(zf, 'manifest.json', dict(
            version=1,
            format=EXPORT_FORMAT,
            scope=scope,
            exported_at=datetime.now(tz=EXPORT_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S'),
            user=dict(id=user.id, username=user.username),
            count=len(articles),
            content_missing=sum(1 for item, _ in articles if item['content_missing']),
        ))
    tmp.seek(0)
    return tmp


def _filename(prefix: str) -> str:
    today = datetime.now(tz=EXPORT_TIMEZONE).strftime('%Y%m%d')
    return '{}-{}.zip'.format(prefix, today)


def build_favorites_zip(user):
    qs = UserStory.objects.select_related('story')\
        .filter(user_id=user.id, is_favorited=True)\
        .order_by('-dt_favorited', '-id')
    total = qs.count()
    articles = []
    for start in range(0, total, FAVORITE_BATCH_SIZE):
        for user_story in qs[start:start + FAVORITE_BATCH_SIZE]:
            story = user_story.story
            articles.append((
                _story_article(story, story.feed_id, '', '', user_story.dt_favorited, ''),
                story,
            ))
    # 补正文文件名和订阅源信息
    feed_ids = {item['feed']['id'] for item, _ in articles}
    feeds = _feed_info_map(user.id, feed_ids)
    for item, _ in articles:
        info = feeds.get(item['feed']['id'])
        if info:
            item['feed']['title'] = info['title']
            item['feed']['url'] = info['url']
    for index, (item, _) in enumerate(articles, start=1):
        item['content_file'] = _content_filename(index, item['title'])
    tmp = _build_zip('favorites', user, articles, _story_markdown)
    return tmp, _filename('rssant-favorites')


def _feed_info_map(user_id, feed_ids):
    if not feed_ids:
        return {}
    result = {}
    for feed in Feed.objects.filter(id__in=feed_ids):
        result[feed.id] = dict(title=feed.title or '', url=feed.url or '')
    qs = UserFeed.objects.filter(user_id=user_id, feed_id__in=feed_ids)
    for user_feed in qs:
        info = result.setdefault(user_feed.feed_id, dict(title='', url=''))
        if user_feed.title:
            info['title'] = user_feed.title
    return result


def build_feed_zip(user, feed_unionid):
    if feed_unionid[0] != user.id:
        raise PermissionDenied('feed does not belong to user')
    union_feed = UnionFeed.get_by_id(FeedUnionId(user.id, feed_unionid[1]))
    feed = union_feed._feed
    begin = max(feed.retention_offset or 0, feed.total_storys - CONFIG.feed_story_retention)
    articles = []
    for offset in range(begin, feed.total_storys, FEED_BATCH_SIZE):
        end = min(offset + FEED_BATCH_SIZE, feed.total_storys)
        storys = STORY_SERVICE.batch_get_by_offset(
            feed.id, list(range(offset, end)), detail=True)
        for story in storys:
            articles.append((
                _story_article(story, feed.id, union_feed.title, feed.url, None, ''),
                story,
            ))
    for index, (item, _) in enumerate(articles, start=1):
        item['content_file'] = _content_filename(index, item['title'])
    feed_data = dict(
        id=feed.id,
        title=union_feed.title,
        url=feed.url,
        group=union_feed.group,
        total_storys=feed.total_storys,
        exported_count=len(articles),
    )
    tmp = _build_zip('feed', user, articles, _story_markdown, extra_files=[('feed.json', feed_data)])
    return tmp, _filename('rssant-feed-{}'.format(feed.id))


class ExportTooFrequent(Exception):
    pass


def _export_cache_key(user):
    return 'rssant:export:{}'.format(user.id)


def acquire_export_cooldown(user):
    if not cache.add(_export_cache_key(user), 1, EXPORT_COOLDOWN_SECONDS):
        raise ExportTooFrequent()


def release_export_cooldown(user):
    cache.delete(_export_cache_key(user))


def export_too_frequent_response():
    return JsonResponse({'message': '导出太频繁，请稍后再试'}, status=429)


def export_response(user, builder):
    try:
        acquire_export_cooldown(user)
    except ExportTooFrequent:
        return export_too_frequent_response()
    try:
        tmp, filename = builder()
    except Exception:
        release_export_cooldown(user)
        raise
    tmp.seek(0, io.SEEK_END)
    size = tmp.tell()
    tmp.seek(0)
    response = FileResponse(
        tmp, content_type='application/zip', as_attachment=True, filename=filename)
    # FileResponse 只对绝对路径的文件设置 Content-Length，TemporaryFile 没有；
    # 不补的话慢网下浏览器看不到总大小和进度。
    response['Content-Length'] = str(size)
    return response
