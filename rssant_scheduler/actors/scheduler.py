import asyncio
import logging

from validr import T
from actorlib import actor, ActorContext


LOG = logging.getLogger(__name__)


@actor('scheduler.healthcheck')
async def do_healthcheck(ctx: ActorContext):
    next_task = ctx.tell('scheduler.healthcheck')
    asyncio.get_event_loop().call_later(60, asyncio.ensure_future, next_task)
    for node in ctx.registery.nodes:
        LOG.info(f'check node {node.name}')
        await ctx.tell('actor.health', {}, dst_node=node.name)


@actor('scheduler.schedule_check_feed')
async def do_schedule_check_feed(ctx):
    next_task = ctx.tell('scheduler.schedule_check_feed')
    asyncio.get_event_loop().call_later(10, asyncio.ensure_future, next_task)
    await ctx.tell('harbor_rss.check_feed')


@actor('scheduler.schedule_clean_feed_creation')
async def do_schedule_clean_feed_creation(ctx):
    next_task = ctx.tell('scheduler.schedule_clean_feed_creation')
    asyncio.get_event_loop().call_later(60, asyncio.ensure_future, next_task)
    await ctx.tell('harbor_rss.clean_feed_creation')


@actor("scheduler.proxy_tell")
async def do_proxy_tell(
    ctx: ActorContext,
    dst: T.str,
    content: T.dict,
):
    await ctx.tell(dst, content)


@actor("scheduler.proxy_ask")
async def do_proxy_ask(
    ctx: ActorContext,
    dst: T.str,
    content: T.dict,
):
    return await ctx.ask(dst, content)
