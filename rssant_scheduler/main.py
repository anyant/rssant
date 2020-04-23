import logging

from actorlib import actor, ActorContext

from rssant_common.actor_helper import start_actor


LOG = logging.getLogger(__name__)


@actor('actor.init')
async def do_init(ctx: ActorContext):
    await ctx.hope('scheduler.load_registery')
    await ctx.hope('scheduler.dns_service_refresh')


if __name__ == "__main__":
    start_actor('scheduler', port=6790)
