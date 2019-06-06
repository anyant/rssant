import re
from collections import namedtuple
from urllib.parse import urljoin
from html2text import HTML2Text

RE_IMG = re.compile(r'<img\s*.*?\s*src="([^"]+?)"', re.I | re.M)

StoryImageIndexItem = namedtuple('StoryImageIndexItem', 'pos, endpos, value')


class StoryImageProcessor:
    def __init__(self, story_url, content):
        self.story_url = story_url
        self.content = content

    def fix_relative_url(self, url):
        if not url.startswith('http://') and not url.startswith('https://'):
            url = urljoin(self.story_url, url)
        return url

    def parse(self) -> [StoryImageIndexItem]:
        if not self.content:
            return
        content = self.content
        image_indexs = []
        pos = 0
        while True:
            match = RE_IMG.search(content, pos=pos)
            if not match:
                break
            img_url = self.fix_relative_url(match.group(1))
            idx = StoryImageIndexItem(*match.span(1), img_url)
            image_indexs.append(idx)
            pos = match.end(1)
        return image_indexs

    def process(self, image_indexs, images) -> str:
        new_image_indexs = []
        for idx in image_indexs:
            new_url = images.get(idx.value)
            if new_url:
                idx = StoryImageIndexItem(idx.pos, idx.endpos, new_url)
            new_image_indexs.append(idx)
        content = self.content
        content_chunks = []
        beginpos = 0
        for pos, endpos, value in new_image_indexs:
            content_chunks.append(content[beginpos: pos])
            content_chunks.append(value)
            beginpos = endpos
        content_chunks.append(content[beginpos:])
        return ''.join(content_chunks)


def story_html_to_text(content):
    h = HTML2Text()
    h.ignore_links = True
    return h.handle(content or "")
