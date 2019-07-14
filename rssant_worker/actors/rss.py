import logging
from validr import T

from actorlib import actor, ActorContext


LOG = logging.getLogger(__name__)


@actor('worker_rss.fetch_story')
def do_fetch_story(ctx: ActorContext, url: T.url):
    LOG.info(f'fetch story {url}')
