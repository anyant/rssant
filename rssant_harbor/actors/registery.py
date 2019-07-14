import logging
from validr import T

from actorlib import actor, ActorContext, NodeSpecSchema

from rssant_api.models import Registery


LOG = logging.getLogger(__name__)


@actor('harbor_registery.save')
def do_save(
    ctx: ActorContext,
    registery_node: NodeSpecSchema,
    nodes: T.list(NodeSpecSchema),
):
    LOG.info('save registery info from {}'.format(registery_node['name']))
    Registery.create_or_update(registery_node, nodes)
