import logging

import tqdm
from django.db import transaction

from rssant_api.models import Feed, Story


LOG = logging.getLogger(__name__)


def run():
    with transaction.atomic():
        feed_ids = [feed.id for feed in Feed.objects.only('id').all()]
        LOG.info('total %s feeds', len(feed_ids))
        num_fixed = 0
        for feed_id in tqdm.tqdm(feed_ids, ncols=80, ascii=True):
            num_reallocate = Story.reallocate_offset(feed_id)
            if num_reallocate > 0:
                num_fixed += 1
        LOG.info('correct %s feeds', num_fixed)
