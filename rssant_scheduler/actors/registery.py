import logging

from actorlib import actor, ActorContext, NodeSpecSchema
from validr import T

from rssant_api.models import Registery
from rssant_common.helper import pretty_format_json
from rssant_common.actor_helper import django_context


LOG = logging.getLogger(__name__)


@actor('scheduler.save_registery')
@django_context
def do_save_registery(ctx: ActorContext):
    LOG.info('save registery info for {}'.format(ctx.registery.registery_node.name))
    registery_node = ctx.registery.registery_node.to_spec()
    nodes = ctx.registery.to_spec()
    Registery.create_or_update(registery_node, nodes)
    ctx.tell('scheduler.boardcast_registery')


@actor('scheduler.load_registery')
@django_context
def do_load_registery(ctx: ActorContext):
    registery_node = ctx.registery.registery_node.name
    LOG.info(f'load registery info for {registery_node}')
    registery = Registery.get(registery_node)
    if registery:
        ctx.registery.update(registery.node_specs)
        title = 'loaded'
    else:
        title = 'current'
    LOG.info(f'{title} registery info:\n' + pretty_format_json(ctx.registery.to_spec()))
    ctx.tell('scheduler.boardcast_registery')


@actor('scheduler.query_registery')
def do_query_registery(ctx: ActorContext) -> T.dict(nodes=T.list(NodeSpecSchema)):
    return dict(nodes=ctx.registery.to_spec())


@actor('scheduler.boardcast_registery')
async def do_boardcast_registery(ctx: ActorContext):
    msg = dict(nodes=ctx.registery.to_spec())
    for node in ctx.registery.nodes:
        if node.name != ctx.registery.current_node.name:
            await ctx.tell('actor.update_registery', msg, dst_node=node.name)


@actor('scheduler.register')
async def do_register(ctx: ActorContext, node: NodeSpecSchema) -> T.dict(nodes=T.list(NodeSpecSchema)):
    LOG.info(f'register node:\n{pretty_format_json(node)}')
    ctx.registery.add(node)
    LOG.info(f'current registery info:\n' + pretty_format_json(ctx.registery.to_spec()))
    await ctx.tell('scheduler.save_registery')
    return dict(nodes=ctx.registery.to_spec())


@actor('scheduler.unregister')
async def do_unregister(ctx: ActorContext, node_name: T.str) -> T.dict(nodes=T.list(NodeSpecSchema)):
    LOG.info(f'unregister node {node_name}')
    ctx.registery.remove(node_name)
    await ctx.tell('scheduler.save_registery')
    return dict(nodes=ctx.registery.to_spec())
