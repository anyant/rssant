import logging

from actorlib import actor, ActorNode, ActorContext, collect_actors
from validr import T


LOG = logging.getLogger(__name__)


@actor('actor.fab_timer', timer='1s')
async def actor_fab_timer(ctx: ActorContext):
    await ctx.tell('actor.fab', dict(limit=100))


@actor('actor.fab')
async def actor_fab(ctx: ActorContext, limit: T.int, a: T.int.default(1), b: T.int.default(1)):
    LOG.info(f'fab {a} {b}')
    if b < limit:
        await ctx.tell('actor.fab', content=dict(limit=limit, a=b, b=a + b))


ACTORS = collect_actors(__name__)


def main():
    app = ActorNode(
        actors=ACTORS,
        port=8083,
        storage_dir_path='data/actorlib_example_fab',
    )
    app.run()


if __name__ == "__main__":
    from rssant_common.logger import configure_logging
    configure_logging(enable_loguru=True, level='DEBUG')
    main()
