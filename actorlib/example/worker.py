import logging

from validr import T
from actorlib import actor, collect_actors, ActorNode, ActorContext, ActorMessage


LOG = logging.getLogger(__name__)


@actor('worker.ping')
def do_ping(ctx: ActorContext, message: T.str):
    LOG.info(ctx.message)
    ctx.send('worker.pong', dict(message=message))


@actor('worker.pong')
async def do_pong(ctx: ActorContext, message: T.str):
    LOG.info(f'receive: {message}')


ACTORS = collect_actors(__name__)


def main():
    app = ActorNode(
        actors=ACTORS,
        port=8082,
        subpath='/api/v1/worker',
        networks=[{
            'name': 'local',
            'url': 'http://127.0.0.1:8082/api/v1/worker',
        }],
        registery_node_spec={
            'name': 'registery',
            'modules': ['registery'],
            'networks': [{
                'name': 'local',
                'url': 'http://127.0.0.1:8081/api/v1/registery',
            }],
        }
    )
    app.executor.submit(ActorMessage(
        content=dict(node=app.registery.current_node.to_spec()),
        dst='registery.register',
        dst_node=app.registery.registery_node.name,
    ))
    app.run()


if __name__ == "__main__":
    main()
