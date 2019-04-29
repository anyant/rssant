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
            storys = list(
                Story.objects.filter(feed_id=feed_id)
                .only('id', 'offset', 'dt_published')
                .order_by('dt_published', 'id')
                .all())
            updates = []
            for offset, story in enumerate(storys):
                if story.offset != offset:
                    story.offset = -offset - 1
                    story.save()
                    updates.append(story)
            for story in updates:
                story.offset = -(story.offset + 1)
                story.save()
