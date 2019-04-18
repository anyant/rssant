import logging

import tqdm
from django.db import transaction, connection

from rssant.helper.content_hash import compute_hash_base64
from rssant_api.models import Feed, Story


LOG = logging.getLogger(__name__)


def query_old_storys_by_feed(feed_id):
    sql = """
    SELECT unique_id, title, link, author, dt_published, dt_updated, summary, content
    FROM rssant_api_story_bak
    WHERE feed_id=%s
    """
    fields = ['unique_id', 'title', 'link', 'author',
              'dt_published', 'dt_updated', 'summary', 'content']
    storys = []
    with connection.cursor() as cursor:
        cursor.execute(sql, [feed_id])
        for row in cursor.fetchall():
            story = dict(zip(fields, row))
            story['content_hash_base64'] = compute_hash_base64(
                story['content'], story['summary'], story['title'])
            storys.append(story)
    return storys


def run():
    with transaction.atomic():
        feed_ids = [feed.id for feed in Feed.objects.only('id').all()]
        LOG.info('total %s feeds', len(feed_ids))
        for feed_id in tqdm.tqdm(feed_ids, ncols=80, ascii=True):
            storys = query_old_storys_by_feed(feed_id)
            Story.bulk_save_by_feed(feed_id, storys)
