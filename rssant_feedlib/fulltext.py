import re
import difflib
from typing import List

from . import processor


sentence_sep_s = [
    r'\r', r'\n',
    r'\.', r',', r';', r'\?', r'!', r':', r'"', r"'", r'\[', r'\]', r'\(', r'\)',
    r'。', r'，', r'；', r'？', r'！', r'：', r'…', r'、',
    r'“', r'”', r'‘', r'’', r'【', r'】', r'《', r'》', r'（', r'）', r'〈', r'〉',
]

RE_SENTENCE_SEP = re.compile(r'(?:\s*(?:{})\s*)+'.format('|'.join(sentence_sep_s)))


def split_sentences(text: str) -> List[str]:
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
    return sentences


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
    for delta in difflib.ndiff(sub_sentences, full_sentences[:max_check]):
        diff_type = delta[0]
        if diff_type == ' ' or diff_type == '+':
            num_positive += 1
        elif diff_type == '-':
            num_positive -= 3
    return max(0.0, min(1.0, num_positive / max_check))


def is_summary(subtext: str, fulltext: str) -> bool:
    return is_summary_prob(subtext, fulltext) > 0.5


def is_fulltext_content(story_content):
    """
    detect whether the full content is already in rss feed.
    """
    if not story_content:
        return False
    if len(story_content) >= 2000:
        return True
    link_count = processor.story_link_count(story_content)
    if link_count >= 5:
        return True
    url_count = processor.story_url_count(story_content)
    if url_count >= 7:
        return True
    image_count = processor.story_image_count(story_content)
    if image_count >= 2:
        return True
    return False
