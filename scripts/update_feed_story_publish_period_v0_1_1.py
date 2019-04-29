import logging

import tqdm
from django.db import transaction

from rssant_api.models import Feed, Story


LOG = logging.getLogger(__name__)


def run():
    with transaction.atomic():
        feed_ids = [feed.id for feed in Feed.objects.only('id').all()]
        LOG.info('total %s feeds', len(feed_ids))
        for feed_id in tqdm.tqdm(feed_ids, ncols=80, ascii=True):
            Story.update_feed_story_publish_period(feed_id)
