from rssant_api.helper import DuplicateFeedDetector, reverse_url


URL_LIST = [
    'https://rsshub.app/v2ex/topics/hot',
    'https://rsshub.app/v2ex/topics/hot.atom?mode=fulltext',
    'https://rsshub.app/v2ex/topics/hot?mode=fulltext',
    'https://rsshub.app/v2ex/topics/hot?mode=fulltext.atom',
    'http://rsshub.app/v2ex/topics/hot',
    'http://rsshub.app/v2ex/topics/hot?mode=fulltext',
    'https://rsshub.ioiox.com/v2ex/topics/hot',
    'https://rsshub.rssforever.com/v2ex/topics/hot',
    'https://datatube.dev/api/rss/v2ex/topics/hot',
    'http://wssgwps.fun:1200/v2ex/topics/hot',
    'https://feed.glaceon.net/v2ex/topics/hot?mode=fulltext',
    'https://rss.ez.rw/v2ex/topics/hot',
]


def test_detect_duplicate_feed():
    detector = DuplicateFeedDetector()
    for index, url in enumerate(URL_LIST):
        detector.push(index, reverse_url(url))
    result_url_s = []
    for id_s in detector.poll():
        result_url_s.append(tuple(URL_LIST[x] for x in id_s))
    assert len(result_url_s) == 2
    assert result_url_s[0] == (
        'https://rsshub.app/v2ex/topics/hot',
        'http://rsshub.app/v2ex/topics/hot',
    )
    assert result_url_s[1] == (
        'https://rsshub.app/v2ex/topics/hot?mode=fulltext',
        'http://rsshub.app/v2ex/topics/hot?mode=fulltext',
    )
