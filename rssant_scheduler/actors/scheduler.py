import asyncio
import logging

from actorlib import actor, ActorContext, NodeSpecSchema


LOG = logging.getLogger(__name__)


@actor('scheduler.register')
async def do_register(ctx: ActorContext, node: NodeSpecSchema):
    LOG.info(f'register node {node}')
    ctx.registery.add(node)
    await ctx.send('harbor_registery.save', dict(
        registery_node=ctx.registery.registery_node.to_spec(),
        nodes=ctx.registery.to_spec(),
    ))
    msg = dict(nodes=ctx.registery.to_spec())
    for node in ctx.registery.nodes:
        if node.name != ctx.registery.current_node.name:
            await ctx.send('actor.update_registery', msg, dst_node=node.name)


@actor('scheduler.healthcheck')
async def do_healthcheck(ctx: ActorContext):
    next_task = ctx.send('scheduler.healthcheck')
    asyncio.get_event_loop().call_later(60, asyncio.ensure_future, next_task)
    for node in ctx.registery.nodes:
        LOG.info(f'check node {node.name}')
        await ctx.send('actor.health', {}, dst_node=node.name)
