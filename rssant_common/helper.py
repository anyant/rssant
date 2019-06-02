import json
import codecs
import cchardet
from terminaltables import AsciiTable


def pretty_format_json(data):
    return json.dumps(data, ensure_ascii=False, indent=4)


def format_table(rows, *, header=None, border=True):
    table_data = []
    if header:
        table_data.append(list(header))
    elif rows:
        table_data.append([f'#{i}' for i in range(len(rows[0]))])
    table_data.extend(rows)
    table = AsciiTable(table_data)
    if not border:
        table.inner_column_border = False
        table.outer_border = False
    return table.table


def _is_encoding_exists(response):
    content_type = response.headers.get('content-type')
    return content_type and 'charset' in content_type


def detect_response_encoding(content):
    # response.apparent_encoding使用chardet检测编码，有些情况会非常慢
    # 换成cchardet实现，性能可以提升100倍
    encoding = cchardet.detect(content)['encoding']
    if encoding:
        encoding = encoding.lower()
        # 解决常见的乱码问题，chardet没检测出来基本就是windows-1254编码
        if encoding == 'windows-1254' or encoding == 'ascii':
            encoding = 'utf-8'
    else:
        encoding = 'utf-8'
    encoding = codecs.lookup(encoding).name
    return encoding


def resolve_response_encoding(response):
    if _is_encoding_exists(response) and response.encoding:
        encoding = codecs.lookup(response.encoding).name
    else:
        encoding = detect_response_encoding(response.content)
    response.encoding = encoding


async def resolve_aiohttp_response_encoding(response, content):
    if _is_encoding_exists(response) and response.charset:
        encoding = codecs.lookup(response.charset).name
    else:
        encoding = detect_response_encoding(content)
    return encoding


def coerce_url(url, default_schema='http'):
    url = url.strip()
    if url.startswith("feed://"):
        return "{}://{}".format(default_schema, url[7:])
    if "://" not in url:
        return "{}://{}".format(default_schema, url)
    return url
