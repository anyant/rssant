import asyncio
import logging
import time
from collections import defaultdict

from validr import T
from actorlib import actor, ActorContext

from rssant_common.dns_service import DNS_SERVICE


LOG = logging.getLogger(__name__)


@actor('scheduler.healthcheck', timer='60s')
async def do_healthcheck(ctx: ActorContext):
    unhealth_count = defaultdict(lambda: 0)
    for i in range(3):
        for node in ctx.registery.remote_nodes:
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
async def do_schedule_check_feed(ctx: ActorContext):
    await ctx.tell('harbor_rss.check_feed', expire_at=time.time() + 30)


@actor('scheduler.schedule_clean_feed_creation', timer='10s')
async def do_schedule_clean_feed_creation(ctx: ActorContext):
    await ctx.tell('harbor_rss.clean_feed_creation', expire_at=time.time() + 30)


@actor('scheduler.schedule_clean_by_retention', timer='1m')
async def do_schedule_clean_by_retention(ctx: ActorContext):
    await ctx.tell('harbor_rss.clean_by_retention', expire_at=time.time() + 180)


@actor('scheduler.schedule_clean_image_info_by_retention', timer='1m')
async def do_schedule_clean_image_info_by_retention(ctx: ActorContext):
    await ctx.tell('harbor_rss.clean_image_info_by_retention', expire_at=time.time() + 180)


@actor('scheduler.schedule_clean_feedurlmap_by_retention', timer='30m')
async def do_schedule_clean_feedurlmap_by_retention(ctx: ActorContext):
    await ctx.tell('harbor_rss.clean_feedurlmap_by_retention', expire_at=time.time() + 180)


@actor('scheduler.schedule_feed_refresh_freeze_level', timer='40m')
async def do_schedule_feed_refresh_freeze_level(ctx: ActorContext):
    await ctx.tell('harbor_rss.feed_refresh_freeze_level', expire_at=time.time() + 600)


@actor('scheduler.schedule_feed_detect_and_merge_duplicate', timer='4h')
async def do_schedule_feed_detect_and_merge_duplicate(ctx: ActorContext):
    await ctx.tell('harbor_rss.feed_detect_and_merge_duplicate', expire_at=time.time() + 600)


@actor('scheduler.dns_service_refresh', timer='4h')
def do_dns_service_refresh(ctx: ActorContext):
    DNS_SERVICE.refresh()
    records = {}
    for host, ip_set in DNS_SERVICE.records.items():
        records[host] = list(ip_set)
    msg = dict(records=records)
    expire_at = time.time() + 60 * 60
    for node in ctx.registery.remote_nodes:
        ctx.tell('actor.dns_service_update', msg, dst_node=node.name, expire_at=expire_at)


@actor("scheduler.proxy_tell")
async def do_proxy_tell(
    ctx: ActorContext,
    tasks: T.list(T.dict(
        dst=T.str,
        content=T.dict.optional,
    )).maxlen(2000)
):
    for t in tasks:
        await ctx.tell(dst=t['dst'], content=t['content'], priority=10)


@actor("scheduler.proxy_ask")
async def do_proxy_ask(
    ctx: ActorContext,
    dst: T.str,
    content: T.dict.optional,
):
    return await ctx.ask(dst, content)
