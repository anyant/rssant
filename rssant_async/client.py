import json
import requests


class RssantAsyncClient:

    def __init__(self, url_prefix, callback_url_prefix):
        self.url_prefix = url_prefix.lstrip('/')
        self.callback_url_prefix = callback_url_prefix.lstrip('/')
        self.session = requests.Session()

    def close(self):
        self.session.close()

    def _get_url(self, url):
        return self.url_prefix + url

    def _get_callback(self, url):
        return self.callback_url_prefix + url

    def _raise_for_status(self, response):
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as ex:
            try:
                data = response.json()
            except (ValueError, json.JSONDecodeError):
                pass  # ignore
            else:
                ex.args += (data,)
            raise

    def fetch_storys(self, storys, callback):
        response = self.session.post(self._get_url('/async/fetch_storys'), json={
            'storys': storys,
            'callback': self._get_callback(callback),
        })
        self._raise_for_status(response)
        return response.json()

    def get_story(self, id):
        response = self.session.get(self._get_url('/async/get_story'), params={
            'id': id,
        })
        self._raise_for_status(response)
        return response.json()

    def detect_story_images(self, story_id, story_url, image_urls, callback):
        images = [{'url': url} for url in image_urls]
        response = self.session.post(self._get_url('/async/detect_story_images'), json={
            'story': {'id': str(story_id), 'url': story_url},
            'images': images,
            'callback': self._get_callback(callback),
        })
        self._raise_for_status(response)
        return response.json()
