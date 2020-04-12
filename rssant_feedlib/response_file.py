import os.path
import json

from rssant_common.helper import pretty_format_json

from .response import FeedResponse, FeedContentType


def _normalize_path(p):
    return os.path.abspath(os.path.expanduser(p))


class FeedResponseFile:

    def __init__(self, filename: str):
        """
        /path/to/filename.feed.json
        /path/to/filename.xml
        """
        filename = str(filename)
        if filename.endswith('.feed.json'):
            filename = filename[:-len('.feed.json')]
        self._filename = filename
        self._meta_filepath = _normalize_path(self._filename + '.feed.json')
        self._output_dir = os.path.dirname(self._meta_filepath)

    @property
    def filepath(self) -> str:
        return self._meta_filepath

    @classmethod
    def _get_file_ext(cls, response: FeedResponse):
        if response.feed_type.is_json:
            return '.json'
        elif response.feed_type.is_html:
            return '.html'
        elif response.feed_type.is_xml:
            return '.xml'
        else:
            return '.txt'

    def write(self, response: FeedResponse):
        content_length = 0
        if response.content:
            content_length = len(response.content)
        feed_type = response.feed_type.value if response.feed_type else None
        filename = None
        if response.content:
            file_ext = self._get_file_ext(response)
            filename = os.path.basename(self._filename) + file_ext
        meta = dict(
            filename=filename,
            url=response.url,
            status=response.status,
            content_length=content_length,
            encoding=response.encoding,
            feed_type=feed_type,
            mime_type=response.mime_type,
            use_proxy=response.use_proxy,
            etag=response.etag,
            last_modified=response.last_modified,
        )
        os.makedirs(self._output_dir, exist_ok=True)
        with open(self._meta_filepath, 'w') as f:
            f.write(pretty_format_json(meta))
        if filename:
            filepath = _normalize_path(os.path.join(self._output_dir, filename))
            with open(filepath, 'wb') as f:
                f.write(response.content)

    def read(self) -> FeedResponse:
        with open(self._meta_filepath) as f:
            meta = json.load(f)
        content = None
        filename = meta.get('filename')
        if filename:
            filepath = os.path.join(self._output_dir, filename)
            with open(filepath, 'rb') as f:
                content = f.read()
        feed_type = meta.get('feed_type')
        feed_type = FeedContentType(feed_type) if feed_type else None
        response = FeedResponse(
            url=meta['url'],
            status=meta['status'],
            content=content,
            encoding=meta['encoding'],
            feed_type=feed_type,
            mime_type=meta['mime_type'],
            use_proxy=meta['use_proxy'],
            etag=meta['etag'],
            last_modified=meta['last_modified'],
        )
        return response
