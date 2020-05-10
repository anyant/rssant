import pytest
import yarl

from .helper import reverse_url, forward_url


reverse_and_forward_url_cases = [
    (
        'https://rss.anyant.com',
        'com.anyant.rss!443!https/'
    ),
    (
        'http://rss.anyant.com',
        'com.anyant.rss!80!http/'
    ),
    (
        'http://rss.anyant.com:8000',
        'com.anyant.rss!8000!http/'
    ),
    (
        'https://rss.anyant.com/changelog',
        'com.anyant.rss!443!https/changelog'
    ),
    (
        'https://rss.anyant.com/changelog?',
        'com.anyant.rss!443!https/changelog'
    ),
    (
        'https://rss.anyant.com/changelog.atom?version=1.0.0',
        'com.anyant.rss!443!https/changelog.atom?version=1.0.0'
    ),
    (
        'https://rss.anyant.com/changelog.atom?version=1.0.0#tag=abc',
        'com.anyant.rss!443!https/changelog.atom?version=1.0.0#tag=abc'
    ),
    (
        'https://rss.anyant.com/博客?版本=1.1.0#tag=abc',
        'com.anyant.rss!443!https/%E5%8D%9A%E5%AE%A2?%E7%89%88%E6%9C%AC=1.1.0#tag=abc'
    ),
    (
        'https://rss.anyant.com/%E5%8D%9A%E5%AE%A2?%E7%89%88%E6%9C%AC=1.1.0#tag=abc',
        'com.anyant.rss!443!https/%E5%8D%9A%E5%AE%A2?%E7%89%88%E6%9C%AC=1.1.0#tag=abc'
    ),
]


@pytest.mark.parametrize('url,rev_url', reverse_and_forward_url_cases)
def test_reverse_and_forward_url(url, rev_url):
    assert reverse_url(url) == rev_url
    assert yarl.URL(forward_url(rev_url)) == yarl.URL(url)
