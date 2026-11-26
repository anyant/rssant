"""添加订阅失败时给用户看的中文描述。

按用户输入地址的首个响应判定；这里只生成文案，状态和清空由 harbor 负责。
"""
from rssant_feedlib import FeedResponse, FeedResponseStatus

NOT_FOUND = '这个地址不存在。检查链接是否写错，或换一个 RSS 地址试试。'
ACCESS_DENIED = '网站拒绝了访问。可以稍后再试，或换一个 RSS 地址。'
TEMPORARY_UNAVAILABLE = '网站暂时不可用。稍后再试。'
SSL_ERROR = '无法安全连接这个网站。换一个 RSS 地址试试。'
CONNECTION_ERROR = '暂时连不上这个网站。检查链接是否写错，或稍后再试。'
PRIVATE_ADDRESS = '这个地址是内网地址，订阅服务访问不到。换成公网地址试试。'
HTML_PAGE = '这个地址是网页，不是订阅源。蚁阅只能订阅 RSS 地址；网站没有 RSS 就无法订阅。'
NOT_A_FEED = '这个地址不是订阅源。换一个 RSS 地址试试。'
UNKNOWN = '没有找到订阅源。换一个 RSS 地址试试。'

_ACCESS_DENIED_STATUS = {
    401,
    403,
    FeedResponseStatus.REFERER_DENY.value,
    FeedResponseStatus.REFERER_NOT_ALLOWED.value,
}

_CONNECTION_ERROR_STATUS = {
    FeedResponseStatus.CONNECTION_ERROR.value,
    FeedResponseStatus.DNS_ERROR.value,
    FeedResponseStatus.CONNECTION_TIMEOUT.value,
    FeedResponseStatus.READ_TIMEOUT.value,
    FeedResponseStatus.CONNECTION_RESET.value,
    FeedResponseStatus.PROXY_ERROR.value,
    FeedResponseStatus.RSS_PROXY_ERROR.value,
    FeedResponseStatus.TOO_MANY_REDIRECT_ERROR.value,
}


def describe_feed_creation_error(response: FeedResponse = None) -> str:
    """按首个响应生成失败描述，无法判定时返回兜底文案"""
    if response is None:
        return UNKNOWN
    status = response.status
    if status in (404, 410):
        return NOT_FOUND
    if status in _ACCESS_DENIED_STATUS:
        return ACCESS_DENIED
    if status == 429 or 500 <= status < 600:
        return TEMPORARY_UNAVAILABLE
    if status == FeedResponseStatus.SSL_ERROR.value:
        return SSL_ERROR
    if status == FeedResponseStatus.PRIVATE_ADDRESS_ERROR.value:
        return PRIVATE_ADDRESS
    if status in _CONNECTION_ERROR_STATUS:
        return CONNECTION_ERROR
    if 200 <= status < 300:
        if response.feed_type.is_html:
            return HTML_PAGE
        return NOT_A_FEED
    return UNKNOWN
