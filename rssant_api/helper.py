import yarl
from collections import defaultdict


def shorten(text, width, placeholder='...'):
    """
    >>> shorten('123456789', width=8)
    '12345...'
    >>> shorten('123456789', width=9)
    '123456789'
    """
    if not text:
        return text
    if len(text) <= width:
        return text
    return text[: max(0, width - len(placeholder))] + placeholder


def reverse_url(url):
    """
    convert url to reversed url
    """
    url = yarl.URL(url)
    host = '.'.join(reversed(url.host.split('.')))
    result = f'{host}!{url.port}!{url.scheme}{url.raw_path_qs}'
    if url.raw_fragment:
        result += '#' + url.raw_fragment
    return result


def forward_url(url):
    """
    convert reversed url to normal url
    """
    try:
        host, port, other = url.split('!', 2)
        scheme, extra = other.split('/', 1)
    except ValueError as ex:
        raise ValueError(f'invalid reverse url: {ex}') from None
    colon_port = ''
    if port == '443' and scheme == 'https':
        colon_port = ''
    elif port == '80' and scheme == 'http':
        colon_port = ''
    else:
        colon_port = ':' + port
    host = '.'.join(reversed(host.split('.')))
    result = f'{scheme}://{host}{colon_port}/{extra}'
    return result


class DuplicateFeedDetector:
    """
    A stream detector to find duplicate feeds,
    assume push feeds by the order of reverse url

    >>> det = DuplicateFeedDetector()
    >>> feeds = [
    ...     (11, 'http://a.example.com/feed.xml'),
    ...     (12, 'https://b.example.com/feed.xml'),
    ...     (21, 'http://rss.anyant.com/changelog.atom'),
    ...     (22, 'https://rss.anyant.com/changelog.atom'),
    ...     (23, 'https://rss.anyant.com/changelog.atom?version=1.0.0'),
    ...     (24, 'https://rss.anyant.com/changelog.atom?'),
    ...     (31, 'http://blog.guyskk.com/feed.xml'),
    ...     (32, 'https://blog.guyskk.com/feed.xml'),
    ... ]
    >>> for feed_id, url in feeds:
    ...    det.push(feed_id, reverse_url(url))
    >>> checkpoint = reverse_url('http://blog.guyskk.com/feed.xml')
    >>> assert det.checkpoint == checkpoint, det.checkpoint
    >>> got = det.poll()
    >>> assert got == [(22, 21, 24)], got
    >>> det.flush()
    >>> got = det.poll()
    >>> assert got == [(32, 31)], got
    >>> assert det.checkpoint is None, det.checkpoint
    """

    def __init__(self):
        self._buffer = []
        self._results = []
        self._last_host = None

    def _is_ignore(self, url_obj: yarl.URL):
        scheme_port = (url_obj.scheme, url_obj.port)
        return scheme_port not in (('http', 80), ('https', 443))

    def _is_primary(self, url_obj: yarl.URL):
        scheme_port = (url_obj.scheme, url_obj.port)
        return scheme_port == ('https', 443)

    def _flush(self):
        cache = defaultdict(list)
        for feed_id, rev_url, url_obj in self._buffer:
            key = url_obj.path_qs
            cache[key].append((feed_id, rev_url, url_obj))
        results = []
        for _, values in cache.items():
            if len(values) >= 2:
                primary = None
                duplicates = []
                for feed_id, rev_url, url_obj in sorted(set(values)):
                    if primary is None and self._is_primary(url_obj):
                        primary = feed_id
                    else:
                        duplicates.append(feed_id)
                results.append((primary, *duplicates))
        self._results.extend(results)
        self._buffer.clear()
        self._last_host = None

    def push(self, feed_id, feed_reverse_url):
        feed_url = forward_url(feed_reverse_url)
        feed_url_obj = yarl.URL(feed_url)
        if self._is_ignore(feed_url_obj):
            return
        if self._last_host is None:
            self._last_host = feed_url_obj.host
        elif self._last_host != feed_url_obj.host:
            self._flush()
            self._last_host = feed_url_obj.host
        self._buffer.append((feed_id, feed_reverse_url, feed_url_obj))

    def flush(self):
        self._flush()

    def poll(self) -> list:
        ret = self._results
        self._results = []
        return ret

    @property
    def checkpoint(self) -> str:
        if self._buffer:
            _, rev_url, _ = self._buffer[0]
            return rev_url
        return None
