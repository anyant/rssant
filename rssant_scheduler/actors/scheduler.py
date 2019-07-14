import asyncio
import logging

from validr import T
from actorlib import actor, ActorContext, NodeSpecSchema

from rssant_api.models import Registery
from rssant_common.helper import pretty_format_json


LOG = logging.getLogger(__name__)


@actor('scheduler.save_registery')
def do_save_registery(ctx: ActorContext):
    LOG.info('save registery info for {}'.format(ctx.registery.registery_node.name))
    registery_node = ctx.registery.registery_node.to_spec()
    nodes = ctx.registery.to_spec()
    Registery.create_or_update(registery_node, nodes)
    ctx.send('scheduler.boardcast_registery')


@actor('scheduler.load_registery')
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
    ctx.send('scheduler.boardcast_registery')


@actor('scheduler.boardcast_registery')
async def do_boardcast_registery(ctx: ActorContext):
    msg = dict(nodes=ctx.registery.to_spec())
    for node in ctx.registery.nodes:
        if node.name != ctx.registery.current_node.name:
            await ctx.send('actor.update_registery', msg, dst_node=node.name)


@actor('scheduler.register')
async def do_register(ctx: ActorContext, node: NodeSpecSchema):
    LOG.info(f'register node {node}')
    ctx.registery.add(node)
    await ctx.send('scheduler.save_registery')


@actor('scheduler.healthcheck')
async def do_healthcheck(ctx: ActorContext):
    next_task = ctx.send('scheduler.healthcheck')
    asyncio.get_event_loop().call_later(60, asyncio.ensure_future, next_task)
    for node in ctx.registery.nodes:
        LOG.info(f'check node {node.name}')
        await ctx.send('actor.health', {}, dst_node=node.name)


@actor('scheduler.schedule_check_feed')
async def do_schedule_check_feed(ctx):
    next_task = ctx.send('scheduler.schedule_check_feed')
    asyncio.get_event_loop().call_later(10, asyncio.ensure_future, next_task)
    await ctx.send('harbor_rss.check_feed')


@actor('scheduler.schedule_clean_feed_creation')
async def do_schedule_clean_feed_creation(ctx):
    next_task = ctx.send('scheduler.schedule_clean_feed_creation')
    asyncio.get_event_loop().call_later(60, asyncio.ensure_future, next_task)
    await ctx.send('harbor_rss.clean_feed_creation')


@actor("scheduler.schedule")
async def do_schedule(
    ctx: ActorContext,
    dst: T.str,
    content: T.dict,
):
    await ctx.send(dst, content)
