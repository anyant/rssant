from rssant_common.helper import pretty_format_json

from actorlib.actor import actor
from actorlib.context import ActorContext

from .name import ACTOR_HEALTH
from .base import BuiltinActorBase


@actor(ACTOR_HEALTH)
class ActorHealth(BuiltinActorBase):

    async def __call__(self, ctx: ActorContext):
        """Report health metrics"""
        return self.health()

    def health(self):
        app = self.app
        # registery
        registery_info = {}
        registery_info['current_node'] = app.registery.current_node.to_spec()
        if app.registery.registery_node:
            registery_info['registery_node'] = app.registery.registery_node.to_spec()
        else:
            registery_info['registery_node'] = None
        registery_info['nodes'] = app.registery.to_spec()
        # queue
        queue_info = dict(
            inbox_size=app.queue.inbox_size(),
            outbox_size=app.queue.outbox_size(),
        )
        # storage
        storage_info = dict()
        # storage_compactor
        storage_compactor_info = {}
        return dict(
            name=self.name,
            host=self.host,
            port=self.port,
            subpath=self.subpath,
            concurrency=self.concurrency,
            registery=registery_info,
            storage=storage_info,
            storage_compactor=storage_compactor_info,
            receiver=dict(),  # TODO: receiver/aiohttp metrics
            queue_info=queue_info,
            executor=dict(
                concurrency=self.executor.concurrency,
                num_async_workers=self.executor.num_async_workers,
                num_thread_workers=self.executor.num_thread_workers,
            ),
            message_monitor=dict(
                ack_timeout=self.message_monitor.ack_timeout,
                max_retry_count=self.message_monitor.max_retry_count,
            )
        )

    def print_health(self):
        print(pretty_format_json(self.health()))
