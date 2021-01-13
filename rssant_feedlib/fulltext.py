import re
import difflib
import enum
from typing import List

from cached_property import cached_property
from wcwidth import wcswidth
from . import processor
from .helper import RE_URL


sentence_sep_s = [
    r'\r', r'\n',
    r'\.', r',', r';', r'\?', r'!', r':', r'"', r"'", r'\[', r'\]', r'\(', r'\)',
    r'。', r'，', r'；', r'？', r'！', r'：', r'…', r'、',
    r'“', r'”', r'‘', r'’', r'【', r'】', r'《', r'》', r'（', r'）', r'〈', r'〉',
]

_SEP_S = r'(?:[\s\d]*(?:{})[\s\d]*)+'.format('|'.join(sentence_sep_s))
RE_SENTENCE_SEP = re.compile(r'(?:{})|(?:{})'.format(RE_URL.pattern, _SEP_S), re.I)


def split_sentences(text: str, keep_short: bool = False) -> List[str]:
    """
    中英文分句
    """
    if not text:
        return []
    sentences = RE_SENTENCE_SEP.split(text)
    if sentences and not sentences[0]:
        sentences = sentences[1:]
    if sentences and not sentences[-1]:
        sentences = sentences[:-1]
    if not keep_short:
        sentences = [x for x in sentences if not is_short_sentence(x)]
    return sentences


def is_short_sentence(sentence: str) -> bool:
    """
    >>> is_short_sentence('')
    True
    >>> is_short_sentence('hello')
    True
    >>> is_short_sentence('你好呀')
    True
    >>> is_short_sentence('hello world')
    False
    >>> is_short_sentence('你好世界!')
    False
    >>> is_short_sentence('aaaa bbbb cccc dddd eeee ffff')
    False
    """
    return len(sentence) <= 16 and wcswidth(sentence) <= 8


def is_summary_prob(subtext: str, fulltext: str) -> float:
    """
    判断subtext是fulltext的摘要的概率。概率大于0.5可认为是。
    """
    sub_sentences = split_sentences(subtext)
    full_sentences = split_sentences(fulltext)
    if not sub_sentences:
        return 1.0 if full_sentences else 0.0
    elif not full_sentences:
        return 0.0
    num_sub = len(sub_sentences)
    num_full = len(full_sentences)
    if num_sub - num_full >= 0:
        return 0.0
    max_check = min(num_sub * 2 + 1, num_full)
    num_positive = 0
    prev_value = 0
    for delta in difflib.ndiff(sub_sentences, full_sentences[:max_check]):
        diff_type = delta[0]
        if diff_type == ' ' or diff_type == '+':
            value = 1
        elif diff_type == '-':
            value = -2.5
        elif diff_type == '?':
            value = 0.5 - prev_value
        else:
            value = 0
        num_positive += value
        prev_value = value
    return max(0.0, min(1.0, num_positive / max_check))


def is_summary(subtext: str, fulltext: str) -> bool:
    return is_summary_prob(subtext, fulltext) > 0.5


class StoryContentInfo:
    def __init__(self, html: str, *, sentence_count: int = None):
        self.html = html
        self._sentence_count = sentence_count

    def __bool__(self):
        return bool(self.html)

    @cached_property
    def text(self) -> str:
        return processor.story_html_to_text(self.html)

    @property
    def length(self) -> int:
        return len(self.html)

    @cached_property
    def link_count(self) -> int:
        return processor.story_link_count(self.html)

    @cached_property
    def image_count(self) -> int:
        return processor.story_image_count(self.html)

    @cached_property
    def url_count(self) -> int:
        return processor.story_url_count(self.html)

    @property
    def sentence_count(self) -> int:
        if self._sentence_count is None:
            self._sentence_count = len(split_sentences(self.text))
        return self._sentence_count


def is_fulltext_content(story_content_info: StoryContentInfo):
    """
    detect whether the full content is already in rss feed.
    """
    if not story_content_info:
        return False
    if story_content_info.length >= 2000:
        return True
    if story_content_info.link_count >= 5:
        return True
    if story_content_info.url_count >= 7:
        return True
    if story_content_info.image_count >= 2:
        return True
    return False


class FulltextAcceptStrategy(enum.Enum):
    REJECT = "REJECT"
    REPLACE = "REPLACE"
    APPEND = "APPEND"


def decide_accept_fulltext(new_info: StoryContentInfo, old_info: StoryContentInfo) -> FulltextAcceptStrategy:
    if not new_info:
        return FulltextAcceptStrategy.REJECT
    is_basic_accept = (
        new_info.length > old_info.length
        and new_info.image_count >= old_info.image_count
        and new_info.link_count >= old_info.link_count
        and new_info.url_count >= old_info.url_count
    )
    if not is_basic_accept:
        return FulltextAcceptStrategy.REJECT
    if is_fulltext_content(old_info):
        is_accept = (
            is_fulltext_content(new_info)
            and new_info.sentence_count > old_info.sentence_count
        )
        if is_accept:
            return FulltextAcceptStrategy.REPLACE
        else:
            return FulltextAcceptStrategy.REJECT
    else:
        if is_summary(old_info.text, new_info.text):
            old_url_match = RE_URL.search(old_info.html)
            # eg: hackernews only has a comments link, but web page not contains the link
            if old_url_match:
                old_url = old_url_match.group(0)
                if old_url not in new_info.html:
                    return FulltextAcceptStrategy.APPEND
            return FulltextAcceptStrategy.REPLACE
        elif is_fulltext_content(new_info):
            return FulltextAcceptStrategy.APPEND
        else:
            return FulltextAcceptStrategy.REJECT
