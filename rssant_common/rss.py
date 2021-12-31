from validr import T
from django.utils import timezone

from rssant.helper.content_hash import compute_hash_base64
from rssant_feedlib.fulltext import split_sentences
from rssant_feedlib.processor import story_html_to_text
from rssant_common.validator import compiler


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

validate_feed = compiler.compile(FeedSchema)
validate_story = compiler.compile(StorySchema)


def get_story_of_feed_entry(data, now=None):
    """
    将 feedlib.FeedResult 的内容，转成 models.Feed 需要的数据
    """
    if now is None:
        now = timezone.now()
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
    return story


def _compute_sentence_count(content: str) -> int:
    return len(split_sentences(story_html_to_text(content)))
