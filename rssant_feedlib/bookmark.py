import re
from urllib.parse import urlparse, urlunparse, unquote

from validr import T, Invalid

from rssant_common.validator import compiler


validate_url = compiler.compile(T.url)

RE_URL = re.compile(
    r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)"  # noqa
)

BLACKLIST_CONTENT = """
google
google.com
youtube.com
facebook.com
amazon.com
wikipedia.org
twitter.com
vk.com
instagram.com
live.com
tmall.com
baidu.com
taobao.com
jd.com
qq.com
sohu.com
sina.com.cn
jd.com
weibo.com
360.cn
yandex.ru
netflix.com
linkedin.com
twitch.tv
list.tmall.com
t.co
pornhub.com
alipay.com
xvideos.com
yahoo.co.jp
ebay.com
microsoft.com
bing.com
ok.ru
imgur.com
bongacams.com
hao123.com
aliexpress.com
mail.ru
whatsapp.com
xhamster.com
xnxx.com
Naver.com
sogou.com
samsung.com
accuweather.com
goo.gl
sm.cn
github.com
"""


def _parse_blacklist():
    lines = set()
    for line in BLACKLIST_CONTENT.strip().splitlines():
        if line.strip():
            lines.add(line.strip())
    items = []
    for line in list(sorted(lines)):
        items.append(r'((.*\.)?{})'.format(line))
    pattern = re.compile('|'.join(items), re.I)
    return pattern


BLACKLIST_RE = _parse_blacklist()


def parse_bookmark(text):
    tmp_urls = set()
    for match in RE_URL.finditer(text):
        tmp_urls.add(match.group(0).strip())
    urls = []
    for url in tmp_urls:
        url = urlparse(url)
        if not BLACKLIST_RE.fullmatch(url.netloc):
            url = unquote(urlunparse(url))
            try:
                url = validate_url(url)
            except Invalid:
                pass  # ignore
            else:
                urls.append(url)
    urls = list(sorted(urls))
    return urls
