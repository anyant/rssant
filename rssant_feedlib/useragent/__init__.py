import os.path
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


_dir = os.path.dirname(__file__)
_filename = 'fake_useragent_{}.json'.format(fake_useragent.VERSION)
_useragent_path = os.path.join(_dir, _filename)

useragent = fake_useragent.UserAgent(
    path=_useragent_path, fallback=_DEFAULT_USER_AGENT)


def DEFAULT_USER_AGENT():
    return str(useragent.random)
