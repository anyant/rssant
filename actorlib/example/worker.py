import logging
import time

import backdoor
from validr import T
from actorlib import actor, collect_actors, ActorNode, ActorContext


LOG = logging.getLogger(__name__)


@actor('actor.init')
def do_init(ctx: ActorContext):
    while True:
        try:
            ctx.ask('registery.register', dict(node=ctx.registery.current_node.to_spec()))
        except Exception as ex:
            LOG.warning(f'ask registery.register failed: {ex}')
        else:
            break
        time.sleep(3)


@actor('worker.ping')
def do_ping(ctx: ActorContext, message: T.str) -> T.dict(message=T.str):
    LOG.info(ctx.message)
    r = ctx.ask('registery.query')
    LOG.info(r)
    ctx.tell('worker.pong', dict(message=message))
    if message == 'error':
        raise ValueError(message)
    return dict(message=message)


@actor('worker.pong')
async def do_pong(ctx: ActorContext, message: T.str) -> T.dict(message=T.str):
    LOG.info(ctx.message)
    r = await ctx.ask('registery.query')
    LOG.info(r)
    if message == 'error':
        raise ValueError(message)
    return dict(message=message)


@actor('worker.local_ask')
def do_local_ask(ctx: ActorContext) -> T.dict(message=T.str):
    LOG.info(ctx.message)
    r = ctx.ask('worker.async_local_ask')
    LOG.info(r)
    return r


@actor('worker.async_local_ask')
async def do_async_local_ask(ctx: ActorContext) -> T.dict(message=T.str):
    LOG.info(ctx.message)
    r = await ctx.ask('worker.sync_local_ask')
    LOG.info(r)
    return r


@actor('worker.sync_local_ask')
def do_sync_local_ask(ctx: ActorContext) -> T.dict(message=T.str):
    LOG.info(ctx.message)
    r = dict(message='local_ask OK')
    LOG.info(r)
    return r


ACTORS = collect_actors(__name__)


def main():
    backdoor.setup()
    app = ActorNode(
        actors=ACTORS,
        port=8085,
        name='worker',
        storage_dir_path='data/actorlib_example_worker',
        storage_compact_wal_delta=100,
        registery_node_spec={
            'name': 'registery',
            'modules': ['registery'],
            'networks': [{
                'name': 'localhost',
                'url': 'http://localhost:8081',
            }],
        },
    )
    app.run()


if __name__ == "__main__":
    from rssant_common.logger import configure_logging
    from actorlib.sentry import sentry_init
    configure_logging(enable_loguru=True, level='DEBUG')
    sentry_init()
    main()
