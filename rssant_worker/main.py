import os
import logging

import django
from validr import T
from actorlib import actor, collect_actors, ActorNode, NodeSpecSchema

from rssant_common.helper import pretty_format_json


os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'rssant.settings')
django.setup()


LOG = logging.getLogger('rssant_worker')


@actor('actor.init')
def do_init(ctx):
    ctx.send('scheduler.register', dict(node=ctx.registery.current_node.to_spec()))


@actor('actor.health')
def do_health(ctx):
    nodes = pretty_format_json(ctx.registery.to_spec())
    LOG.info(f'receive healthcheck message {ctx.message}:\n{nodes}')


@actor('actor.update_registery')
def do_update_registery(ctx, nodes: T.list(NodeSpecSchema)):
    LOG.info(f'update registery {ctx.message}')
    ctx.registery.update(nodes)


ACTORS = collect_actors('rssant_worker')


app = ActorNode(
    actors=ACTORS,
    port=6792,
    name='worker',
    subpath='/api/v1/worker',
    networks=[{
        'name': 'local',
        'url': 'http://127.0.0.1:6792/api/v1/worker',
    }],
    registery_node_spec={
        'name': 'scheduler',
        'modules': ['scheduler'],
        'networks': [{
            'name': 'local',
            'url': 'http://127.0.0.1:6790/api/v1/scheduler',
        }]
    }
)


if __name__ == "__main__":
    print(pretty_format_json(app.registery.to_spec()))
    app.run()
