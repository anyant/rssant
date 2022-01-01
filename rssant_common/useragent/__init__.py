import os.path
from urllib.parse import urlsplit
import fake_useragent


_DEFAULT_RSSANT_USER_AGENT = (
    'Mozilla/5.0 (X11; Linux x86_64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/67.0.3396.87 Safari/537.36 RSSAnt/1.0'
)


_DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Linux; Android 8.0.0; TA-1053 Build/OPR1.170623.026) '
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3368.0 Mobile Safari/537.36'
)

# https://werss.app/help
WERSS_USER_AGENT = 'Mozilla/5.0 (compatible; RSSAnt)'

_dir = os.path.dirname(__file__)
_filename = 'fake_useragent_{}.json'.format(fake_useragent.VERSION)
_useragent_path = os.path.join(_dir, _filename)


class FakeUserAgent:
    """
    Avoid warning: Error occurred during getting browser, but was suppressed with fallback.
    https://github.com/hellysmile/fake-useragent/blob/master/fake_useragent/fake.py#L142
    """

    def __init__(self):
        self._useragent = fake_useragent.UserAgent(
            path=_useragent_path, fallback=_DEFAULT_USER_AGENT)

    def random(self):
        return str(self._useragent.random)


useragent = FakeUserAgent()


def DEFAULT_USER_AGENT(target=None):
    """
    >>> ua = DEFAULT_USER_AGENT('https://cdn.werss.weapp.design')
    >>> assert ua == WERSS_USER_AGENT
    """
    if target:
        host = urlsplit(target).hostname
        # eg: https://cdn.werss.weapp.design/api/v1/feeds/xxxx.xml
        if host and 'werss.' in host.lower():
            return WERSS_USER_AGENT
    return str(useragent.random())
