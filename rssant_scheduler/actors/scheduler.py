import asyncio
import logging
from collections import defaultdict

from validr import T
from actorlib import actor, ActorContext


LOG = logging.getLogger(__name__)


@actor('scheduler.healthcheck', timer='60s')
async def do_healthcheck(ctx: ActorContext):
    unhealth_count = defaultdict(lambda: 0)
    for i in range(3):
        for node in ctx.registery.nodes:
            try:
                await ctx.ask('actor.health', dst_node=node.name)
            except Exception as ex:
                LOG.warning(f'{node.name} not health: {ex}')
                unhealth_count[node.name] += 1
        await asyncio.sleep(10)
    for name, cnt in unhealth_count.items():
        if cnt >= 3:
            LOG.warning(f'node {name} not health in 3 checks, will unregister it!')
            await ctx.hope('scheduler.unregister', content=dict(node_name=name))


@actor('scheduler.schedule_check_feed', timer='10s')
async def do_schedule_check_feed(ctx):
    await ctx.hope('harbor_rss.check_feed')


@actor('scheduler.schedule_clean_feed_creation', timer='10s')
async def do_schedule_clean_feed_creation(ctx):
    await ctx.hope('harbor_rss.clean_feed_creation')


@actor("scheduler.proxy_tell")
async def do_proxy_tell(
    ctx: ActorContext,
    tasks: T.list(T.dict(
        dst = T.str,
        content = T.dict.optional,
    ))
):
    for t in tasks:
        await ctx.tell(t['dst'], t['content'])
    return None


@actor("scheduler.proxy_ask")
async def do_proxy_ask(
    ctx: ActorContext,
    dst: T.str,
    content: T.dict.optional,
):
    return await ctx.ask(dst, content)
