import pytest
from django.test import TestCase

from rssant_config import CONFIG
from rssant_api.models.story_storage import PostgresClient, PostgresStoryStorage


FEED_IDS = [123, 66_000]
CONTENTS = {
    'empty': None,
    'simple': 'hello world\n你好世界\n',
}


@pytest.mark.dbtest
class PostgresStoryStorageTestCase(TestCase):

    def test_story_storage(self):
        for feed_id in FEED_IDS:
            for content_name in CONTENTS:
                self._test_story_storage(feed_id, content_name)

    def setUp(self):
        db = dict(
            user=CONFIG.pg_user,
            password=CONFIG.pg_password,
            host=CONFIG.pg_host,
            port=CONFIG.pg_port,
            db='test_' + CONFIG.pg_db,
        )
        volumes = {
            0: dict(**db, table='story_volume_0'),
            1: dict(**db, table='story_volume_1'),
        }
        self.client = PostgresClient(volumes)

    def tearDown(self):
        self.client.close()

    def _test_story_storage(self, feed_id, content_name):
        storage = PostgresStoryStorage(self.client)
        content = CONTENTS[content_name]
        storage.save_content(feed_id, 234, content)
        got = storage.get_content(feed_id, 234)
        assert got == content
        storage.delete_content(feed_id, 234)
        got = storage.get_content(feed_id, 234)
        assert got is None
