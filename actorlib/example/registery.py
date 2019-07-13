import asyncio
import logging

from actorlib import actor, collect_actors, ActorNode, ActorContext, NodeSpecSchema


LOG = logging.getLogger(__name__)


@actor('registery.register')
def do_register(ctx: ActorContext, node: NodeSpecSchema):
    LOG.info(f'register node {node}')
    ctx.registery.add(node)
    ctx.send('registery.check', dict(node=node))


@actor('registery.check')
async def do_check(ctx: ActorContext, node: NodeSpecSchema):
    LOG.info('ping node {}'.format(node['name']))
    await ctx.send('worker.ping', {'message': 'ping'}, dst_node=node['name'])
    await asyncio.sleep(10)
    await ctx.send('registery.check', dict(node=node))


ACTORS = collect_actors(__name__)


def main():
    app = ActorNode(
        actors=ACTORS,
        port=8081,
        name='registery',
        subpath='/api/v1/registery',
        networks=[{
            'name': 'local',
            'url': 'http://127.0.0.1:8081/api/v1/registery',
        }])
    app.run()


if __name__ == "__main__":
    main()
