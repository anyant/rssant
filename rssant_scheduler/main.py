import os
import logging

import django
from actorlib import actor, collect_actors, ActorNode, ActorContext
from actorlib.sentry import sentry_init

from rssant_common.helper import pretty_format_json
from rssant_common.validator import compiler as schema_compiler
from rssant_common.logger import configure_logging
from rssant.settings import ENV_CONFIG


configure_logging()
LOG = logging.getLogger(__name__)

if ENV_CONFIG.sentry_enable:
    sentry_init(ENV_CONFIG.sentry_dsn)

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')
django.setup()


@actor('actor.init')
def do_init(ctx: ActorContext):
    ctx.tell('scheduler.load_registery')
    ctx.tell('scheduler.schedule_check_feed')
    ctx.tell('scheduler.schedule_clean_feed_creation')
    # ctx.tell('scheduler.healthcheck')


@actor('actor.health')
def do_health(ctx):
    nodes = pretty_format_json(ctx.registery.to_spec())
    LOG.info(f'receive healthcheck message {ctx.message}:\n{nodes}')


ACTORS = collect_actors('rssant_scheduler')


if __name__ == "__main__":
    ActorNode.cli(
        actors=ACTORS,
        concurrency=100,
        port=6790,
        name='scheduler',
        subpath='/api/v1/scheduler',
        registery_node_spec=ENV_CONFIG.registery_node_spec,
        schema_compiler=schema_compiler,
    )
