import re
from urllib.parse import urlparse


def _parse_blacklist(text):
    lines = set()
    for line in text.strip().splitlines():
        if line.strip():
            lines.add(line.strip())
    items = []
    for line in list(sorted(lines)):
        items.append(r'((.*\.)?{})'.format(line))
    pattern = re.compile('|'.join(items), re.I)
    return pattern


def compile_url_blacklist(text):
    black_re = _parse_blacklist(text)

    def is_in_blacklist(url):
        url = urlparse(url)
        return black_re.fullmatch(url.netloc)

    return is_in_blacklist
