from xml.etree import ElementTree
from urllib.parse import unquote

from .schema import validate_opml


def parse_opml(text):
    result = {}
    result['items'] = items = []
    root = ElementTree.fromstring(text)
    title = root.find('./head/title')
    if title is not None:
        title = title.text
    else:
        title = ''
    result['title'] = title
    for node in root.findall('./body/outline'):
        url = node.attrib.get('xmlUrl')
        items.append({
            'title': node.attrib.get('title'),
            'type': node.attrib.get('type'),
            'url': unquote(url),
        })
    result = validate_opml(result)
    return result
