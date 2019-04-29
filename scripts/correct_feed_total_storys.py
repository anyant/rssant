import logging

import tqdm
from django.db import transaction

from rssant_api.models import Feed, Story


LOG = logging.getLogger(__name__)


def run():
    with transaction.atomic():
        feeds = list(Feed.objects.only('id', 'total_storys').all())
        LOG.info('total %s feeds', len(feeds))
        num_corrected = 0
        for feed in tqdm.tqdm(feeds, ncols=80, ascii=True):
            total_storys = Story.objects.filter(feed_id=feed.id).count()
            if feed.total_storys != total_storys:
                feed.total_storys = total_storys
                feed.save()
                num_corrected += 1
        LOG.info('correct %s feeds', num_corrected)
